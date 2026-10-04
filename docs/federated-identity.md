# Federated sign-in

Strata supports OpenID Connect authorization-code sign-in with S256 PKCE. An administrator explicitly links the provider's immutable `sub` to an existing Strata user. Email addresses, display names and provider groups do not grant access. Projects, memberships and quotas continue to come from Strata.

## Configuration and provisioning

Upgrade through migration `0010`, enable individual identity, and retain an enabled local platform administrator for recovery. Register a client at your identity provider with one exact callback URI, `https://YOUR_STRATA_HOST/auth/oidc/callback`, the authorization-code flow, the `openid` scope and S256 PKCE. Configure:

```sh
STRATA_IDENTITY_ENABLED=true
STRATA_OIDC_ISSUER=https://YOUR_PROVIDER/REALM
STRATA_OIDC_CLIENT_ID=YOUR_REGISTERED_CLIENT
STRATA_OIDC_REDIRECT_URI=https://YOUR_STRATA_HOST/auth/oidc/callback
STRATA_OIDC_TOKEN_AUTH_METHOD=none
STRATA_OIDC_STATE_ENCRYPTION_KEY=YOUR_FERNET_KEY
```

Generate a Fernet key with `python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`. Supply it and any client secret through your deployment's secret store. Replicas must share the encryption key. Changing it invalidates in-progress sign-ins. A public client uses `none` without a secret. A confidential client sets `client_secret_basic` or `client_secret_post` explicitly and supplies `STRATA_OIDC_CLIENT_SECRET`; the method must be advertised by provider discovery.

Issuer, callback and provider endpoint URLs require HTTPS in production. Development HTTP is restricted to loopback. Discovery, token and JWKS endpoints must have the issuer's origin; separately hosted endpoints require an explicit JSON array in `STRATA_OIDC_ALLOWED_ORIGINS`. Entries contain an origin only, such as `["https://keys.example.com"]`. Redirects and environment proxies are disabled for back-channel requests.

Create the local user and grant the intended project membership first. As a platform administrator, use **Account & projects → Federated identities**, or:

```sh
strata auth link-oidc USER_ID PROVIDER_SUBJECT
strata auth oidc-identities
strata auth disable-oidc IDENTITY_ID
```

The provider subject is case-sensitive and belongs to the exact configured issuer. A subject cannot be moved to another user through a link request. Disabling a link also revokes all existing Strata credentials for that account. Account disablement and membership changes remain effective on subsequent requests.

## Sign-in and validation

The login dialog exposes **Sign in with your identity provider** when configured. State and nonce are random and stored as hashes; the PKCE verifier is encrypted. A browser-bound HttpOnly cookie prevents exchanging another browser's response. State expires after ten minutes and can be consumed once, including rejected token exchanges. Login starts have durable per-address and global capacity limits.

Strata verifies an RS256 or ES256 signature against the provider's published signing key, exact issuer, client audience, `azp` when present, expiry, issued-at time and nonce. This profile accepts a single audience equal to the client ID. If `at_hash` is present, it must match the access token. Unknown signing keys trigger a JWKS refresh. Responses, token size, network time and key counts are bounded. The provider token is never returned to the browser or retained in the database.

The callback returns a short-lived HttpOnly handoff cookie, then the workspace exchanges it once for a native revocable Strata session. The exchange requires the configured workspace origin. Native bearer tokens are absent from redirect URLs. Browser sessions use session storage and the configured Strata lifetime, eight hours by default.

Disable query-string logging for `/auth/oidc/callback` in reverse proxies and observability pipelines: authorization codes arrive in that URL. The coordinator container disables Uvicorn access logging. Use HTTPS at the public API boundary and preserve the callback host and scheme.

Sign out revokes the Strata session. Provider SSO cookies can remain active. Provider refresh tokens, automatic account/group provisioning, RP-initiated logout and back-channel single logout are not implemented. A provider-side account suspension does not itself revoke an already issued Strata session; disable the linked Strata account or link for immediate revocation.

## Evidence

Integration checks use a real local HTTP provider and RSA signatures, reject invalid claims and browser/state reuse, verify client authentication and exercise simultaneous PostgreSQL callbacks. A separate local Keycloak 26.8.0 container also completed browser sign-in into an explicitly linked operator account and its assigned project. The test account, realm and project are synthetic. This demonstrates local interoperability; a production provider and deployment still require configuration and validation.

The validation profile follows the [OpenID Connect Core specification](https://openid.net/specs/openid-connect-core-1_0.html). The local provider uses the official [Keycloak container](https://www.keycloak.org/server/containers).
