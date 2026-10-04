import base64
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import jwt
import pytest
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.config import Settings
from control_plane.identity import hash_token
from control_plane.models import OIDCState
from control_plane.oidc import OIDCService
from control_plane.services import DomainError
from tests.integration.test_identity import headers, prepare


@pytest.fixture
def provider():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())) | {
        "kid": "test-key",
        "alg": "RS256",
        "use": "sig",
    }
    codes, overrides, requests = {}, {}, []

    class Provider(BaseHTTPRequestHandler):
        def send(self, value, status=200):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(value).encode())

        def do_GET(self):
            if self.path.endswith("openid-configuration"):
                self.send(
                    {
                        "issuer": issuer,
                        "authorization_endpoint": issuer + "/authorize",
                        "token_endpoint": issuer + "/token",
                        "jwks_uri": issuer + "/keys",
                        "code_challenge_methods_supported": ["S256"],
                        "token_endpoint_auth_methods_supported": [
                            "none",
                            "client_secret_basic",
                            "client_secret_post",
                        ],
                    }
                )
            else:
                self.send({"keys": [public]})

        def do_POST(self):
            body = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
            auth = codes.pop(body["code"][0], None)
            requests.append(body)
            body["authorization_header"] = [self.headers.get("Authorization", "")]
            if (
                auth is None
                or base64.urlsafe_b64encode(
                    hashlib.sha256(body["code_verifier"][0].encode()).digest()
                )
                .decode()
                .rstrip("=")
                != auth["code_challenge"][0]
            ):
                self.send({"error": "invalid_grant"}, 400)
                return
            now = int(time.time())
            claims = {
                "iss": issuer,
                "sub": "alice-subject",
                "aud": "strata-test",
                "iat": now,
                "exp": now + 300,
                "nonce": auth["nonce"][0],
                **overrides,
            }
            self.send(
                {
                    "id_token": jwt.encode(
                        claims, key, algorithm="RS256", headers={"kid": "test-key"}
                    ),
                    "access_token": "synthetic-provider-access-token",
                    "token_type": "Bearer",
                }
            )

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    issuer = f"http://127.0.0.1:{server.server_port}/realm"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield issuer, codes, overrides, requests
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def configured(service, provider):
    service.settings.oidc_issuer = provider[0]
    service.settings.oidc_client_id = "strata-test"
    service.settings.oidc_redirect_uri = "http://127.0.0.1:58000/auth/oidc/callback"
    service.settings.oidc_state_encryption_key = Fernet.generate_key().decode()
    client, root, token, alpha, beta, users, tokens = prepare(service)
    response = client.post(
        "/auth/oidc/identities",
        headers=headers(token),
        json={"user_id": users["alice"], "subject": "alice-subject"},
    )
    assert response.status_code == 201, response.text
    return client, token, users, response.json()


def authorization(client, provider):
    start = client.get("/auth/oidc/start", follow_redirects=False)
    assert start.status_code == 303, start.text
    assert (
        "HttpOnly" in start.headers["set-cookie"] and "SameSite=lax" in start.headers["set-cookie"]
    )
    values = parse_qs(urlsplit(start.headers["location"]).query)
    code = "code-" + values["state"][0]
    provider[1][code] = values
    return values["state"][0], code, values


def test_verified_oidc_handoff_is_browser_bound_one_time_and_revocable(service, provider):
    client, token, users, identity = configured(service, provider)
    state, code, values = authorization(client, provider)
    with service.factory() as session:
        row = session.get(OIDCState, hash_token(state))
        verifier = (
            Fernet(service.settings.oidc_state_encryption_key.encode())
            .decrypt(row.encrypted_verifier.encode())
            .decode()
        )
        assert verifier not in row.encrypted_verifier and state != row.state_hash
    callback = client.get(
        "/auth/oidc/callback", params={"state": state, "code": code}, follow_redirects=False
    )
    assert callback.headers["location"] == "/?sso=complete"
    assert "access_token" not in callback.headers["location"]
    assert (
        client.post(
            "/auth/oidc/session", headers={"Origin": "https://attacker.example"}
        ).status_code
        == 403
    )
    exchange = client.post("/auth/oidc/session", headers={"Origin": "http://127.0.0.1:58000"})
    assert exchange.status_code == 200, exchange.text
    credential = exchange.json()["access_token"]
    assert (
        client.get("/auth/me", headers=headers(credential)).json()["user"]["id"] == users["alice"]
    )
    assert (
        client.post("/auth/oidc/session", headers={"Origin": "http://127.0.0.1:58000"}).status_code
        == 401
    )
    assert (
        client.delete(f"/auth/oidc/identities/{identity['id']}", headers=headers(token)).status_code
        == 204
    )
    assert client.get("/auth/me", headers=headers(credential)).status_code == 401
    assert provider[3][0]["code_verifier"] == [verifier]


@pytest.mark.parametrize(
    "claims",
    [
        {"aud": "another-client"},
        {"iss": "https://wrong.example"},
        {"nonce": "wrong"},
        {"exp": 1},
        {"azp": "another-client"},
        {"aud": ["strata-test", "untrusted"]},
        {"at_hash": "wrong"},
        {"sub": "unprovisioned"},
        {"iat": int(time.time()) + 3600},
        {"nonce": None},
    ],
)
def test_invalid_claims_and_unprovisioned_subject_cannot_create_session(service, provider, claims):
    client, *_ = configured(service, provider)
    provider[2].update(claims)
    state, code, _ = authorization(client, provider)
    response = client.get(
        "/auth/oidc/callback", params={"state": state, "code": code}, follow_redirects=False
    )
    assert response.headers["location"].startswith("/?sso=failed")
    assert (
        client.post("/auth/oidc/session", headers={"Origin": "http://127.0.0.1:58000"}).status_code
        == 401
    )


def test_browser_state_swap_expiry_and_account_link_privileges(service, provider):
    client, token, users, _ = configured(service, provider)
    state, code, _ = authorization(client, provider)
    stranger = TestClient(create_app(service.settings, service))
    response = stranger.get(
        "/auth/oidc/callback", params={"state": state, "code": code}, follow_redirects=False
    )
    assert response.headers["location"].endswith("code=401")
    service.clock.advance(601)
    response = client.get(
        "/auth/oidc/callback", params={"state": state, "code": code}, follow_redirects=False
    )
    assert response.headers["location"].endswith("code=401")
    assert (
        client.post(
            "/auth/oidc/identities",
            headers=headers(token),
            json={"user_id": users["bob"], "subject": "alice-subject"},
        ).status_code
        == 409
    )
    disabled = OIDCService(service)
    service.settings.oidc_issuer = ""
    with pytest.raises(DomainError, match="disabled"):
        disabled.start("127.0.0.1")


@pytest.mark.postgres
def test_concurrent_oidc_callbacks_consume_state_once(postgres_service, provider):
    client, *_ = configured(postgres_service, provider)
    state, code, _ = authorization(client, provider)
    browser = client.cookies.get("strata_oidc_browser")
    start = threading.Barrier(2)

    def callback(_):
        start.wait(timeout=10)
        try:
            return OIDCService(postgres_service).callback(state, browser, code)
        except DomainError as exc:
            assert exc.code == 401
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert len([result for result in pool.map(callback, range(2)) if result]) == 1
    assert len(provider[3]) == 1


@pytest.mark.parametrize("method", ["client_secret_basic", "client_secret_post"])
def test_confidential_clients_authenticate_at_the_token_endpoint(service, provider, method):
    client, *_ = configured(service, provider)
    service.settings.oidc_client_secret = "synthetic secret:with punctuation"
    service.settings.oidc_token_auth_method = method
    state, code, _ = authorization(client, provider)
    response = client.get(
        "/auth/oidc/callback", params={"state": state, "code": code}, follow_redirects=False
    )
    assert response.headers["location"] == "/?sso=complete"
    body = provider[3][0]
    if method == "client_secret_basic":
        assert "client_secret" not in body
        header = body["authorization_header"][0]
        assert base64.b64decode(header.removeprefix("Basic ")).decode() == (
            "strata-test:synthetic+secret%3Awith+punctuation"
        )
    else:
        assert body["client_secret"] == [service.settings.oidc_client_secret]
        assert body["authorization_header"] == [""]


def test_rejected_codes_are_one_time_and_login_starts_are_bounded(service, provider):
    client, *_ = configured(service, provider)
    state, code, _ = authorization(client, provider)
    provider[1].pop(code)
    for _ in range(2):
        response = client.get(
            "/auth/oidc/callback", params={"state": state, "code": code}, follow_redirects=False
        )
        assert response.headers["location"].endswith("code=401")
    assert len(provider[3]) == 1
    service.settings.login_attempt_limit = 1
    for _ in range(9):
        assert client.get("/auth/oidc/start", follow_redirects=False).status_code == 303
    assert client.get("/auth/oidc/start", follow_redirects=False).status_code == 429


def test_oidc_configuration_rejects_ambiguous_credentials_and_endpoint_paths():
    values = {
        "identity_enabled": True,
        "oidc_issuer": "https://idp.example/realm",
        "oidc_client_id": "strata",
        "oidc_redirect_uri": "https://strata.example/auth/oidc/callback",
        "oidc_state_encryption_key": Fernet.generate_key().decode(),
    }
    with pytest.raises(ValueError, match="token auth method"):
        Settings(**values, oidc_client_secret="secret")
    with pytest.raises(ValueError, match="contain a path"):
        Settings(**values, oidc_allowed_origins=["https://other.example/token"])
    settings = Settings(**values, oidc_allowed_origins=["https://other.example/"])
    assert settings.oidc_allowed_origins == ["https://other.example"]
