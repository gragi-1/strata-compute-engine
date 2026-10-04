"""Explicit identity administration and scoped automation credentials."""

import typer


def install(app: typer.Typer) -> None:
    from cli.main import display, request

    auth = typer.Typer(help="Manage your individual session and access tokens.")
    projects = typer.Typer(help="List and administer projects and memberships.")
    accounts = typer.Typer(help="Administer individual user accounts.")
    app.add_typer(auth, name="auth")
    app.add_typer(projects, name="projects")
    app.add_typer(accounts, name="users")

    @auth.command("login")
    def login(username: str) -> None:
        """Prompt securely and return a session token; set STRATA_ACCESS_TOKEN to use it."""
        password = typer.prompt("Password", hide_input=True)
        display(
            request("POST", "/auth/login", json={"username": username, "password": password}).json()
        )

    @auth.command("logout")
    def logout() -> None:
        request("POST", "/auth/logout")
        typer.echo("Session revoked. Clear STRATA_ACCESS_TOKEN in your shell.")

    @auth.command("me")
    def me() -> None:
        display(request("GET", "/auth/me").json())

    @auth.command("oidc-identities")
    def oidc_identities() -> None:
        display(request("GET", "/auth/oidc/identities").json())

    @auth.command("link-oidc")
    def link_oidc(user_id: str, subject: str) -> None:
        display(
            request(
                "POST", "/auth/oidc/identities", json={"user_id": user_id, "subject": subject}
            ).json()
        )

    @auth.command("disable-oidc")
    def disable_oidc(identity_id: str) -> None:
        request("DELETE", f"/auth/oidc/identities/{identity_id}")
        typer.echo("Federated identity disabled and existing user credentials revoked.")

    @auth.command("password")
    def password() -> None:
        current = typer.prompt("Current password", hide_input=True)
        new = typer.prompt("New password", hide_input=True, confirmation_prompt=True)
        request("POST", "/auth/password", json={"current_password": current, "new_password": new})
        typer.echo("Password changed and existing credentials revoked. Sign in again.")

    @auth.command("tokens")
    def tokens() -> None:
        display(request("GET", "/auth/tokens").json())

    @auth.command("issue-token")
    def issue(
        name: str, project_id: str, role: str = "operator", lifetime_seconds: int = 86400
    ) -> None:
        display(
            request(
                "POST",
                "/auth/tokens",
                json={
                    "name": name,
                    "project_id": project_id,
                    "role": role,
                    "lifetime_seconds": lifetime_seconds,
                },
            ).json()
        )

    @auth.command("revoke-token")
    def revoke(token_id: str) -> None:
        request("DELETE", f"/auth/tokens/{token_id}")
        typer.echo("Credential revoked.")

    @projects.command("list")
    def listing() -> None:
        display(request("GET", "/projects").json())

    @projects.command("create")
    def create(name: str, description: str = "") -> None:
        display(
            request("POST", "/projects", json={"name": name, "description": description}).json()
        )

    @projects.command("members")
    def members(project_id: str) -> None:
        display(request("GET", f"/projects/{project_id}/members").json())

    @projects.command("grant")
    def grant(project_id: str, user_id: str, role: str = "operator") -> None:
        display(
            request("PUT", f"/projects/{project_id}/members/{user_id}", json={"role": role}).json()
        )

    @projects.command("remove-member")
    def remove(project_id: str, user_id: str) -> None:
        request("DELETE", f"/projects/{project_id}/members/{user_id}")
        typer.echo("Membership removed.")

    @projects.command("audit")
    def audit(project_id: str, after: int = 0) -> None:
        display(request("GET", f"/projects/{project_id}/audit", params={"after": after}).json())

    @accounts.command("list")
    def users() -> None:
        display(request("GET", "/auth/users").json())

    @accounts.command("create")
    def user(username: str, display_name: str = "", admin: bool = False) -> None:
        password = typer.prompt("Initial password", hide_input=True, confirmation_prompt=True)
        display(
            request(
                "POST",
                "/auth/users",
                json={
                    "username": username,
                    "password": password,
                    "display_name": display_name,
                    "is_admin": admin,
                },
            ).json()
        )

    @accounts.command("disable")
    def disable(user_id: str) -> None:
        display(request("PATCH", f"/auth/users/{user_id}", json={"enabled": False}).json())

    @accounts.command("reset-password")
    def reset(user_id: str) -> None:
        password = typer.prompt("New password", hide_input=True, confirmation_prompt=True)
        display(request("PATCH", f"/auth/users/{user_id}", json={"password": password}).json())
