# Individual identity and projects

Strata v3 adds individual accounts, revocable opaque sessions, project membership, project-scoped automation tokens, private jobs/datasets/results, project quotas and an actor-specific audit trail. All existing v2 rows remain intact; their nullable ownership is deliberate. Enabling individual identity hides unowned legacy data until a local administrator explicitly adopts it.

## Upgrade and bootstrap

Stop API/RPC, schedulers and workers, take a verified backup, and run `alembic upgrade head`. Create the first administrator locally with `strata-admin bootstrap-admin USERNAME`; the command prompts for a password without putting it in shell history. It refuses any database that already contains users. Set `STRATA_IDENTITY_ENABLED=true`, then start the API. Readiness remains unavailable until an enabled platform administrator exists.

Sign in on the web or run `strata auth login USERNAME`. Put the returned credential in `STRATA_ACCESS_TOKEN` for CLI/SDK calls. Sessions expire after eight hours by default; change `STRATA_SESSION_LIFETIME_SECONDS` to configure it. Password changes and administrator resets revoke all existing credentials. Disabling an account prevents authentication immediately. `strata auth logout` revokes the current session.

The first administrator creates a project, creates user accounts and grants membership through **Account & projects**, the API, or `strata projects` / `strata users`. Roles are `viewer`, `operator` and `admin`. Project administrators manage membership and read their project's audit; platform administrators also manage users, project budgets and workers. The final platform/project administrator cannot be removed through those APIs.

To preserve access to old v2 resources, stop workers and schedulers, wait for their liveness window to expire, and finish/recover any active attempts. Run `strata-admin adopt-legacy PROJECT_ID`. This moves all unowned resources into the chosen project in one transaction, preserving IDs and names and moving idempotency keys into the project namespace. It refuses live workers or active attempts. Back up before adoption; moving data into a different project requires an explicit reviewed migration.

## Project selection and credentials

Web users select the active project in the header. CLI users set `STRATA_PROJECT_ID`; SDK users pass `Client(access_token=TOKEN, project_id=PROJECT_ID)` or call `select_project`. Business endpoints require `X-Strata-Project` with individual identity enabled. Resources from another project return `404`, including nested routes, inputs, downloads and mutations. Idempotency keys are independent across projects.

Use `strata auth issue-token NAME PROJECT_ID --role operator` for automation. The secret appears once. Tokens always belong to one project and cannot exceed current membership privileges. Membership removal or downgrading takes effect on subsequent requests, including existing tokens. Tokens expire and can be revoked individually; list metadata with `strata auth tokens` and revoke with `strata auth revoke-token TOKEN_ID`. Protect credentials like passwords and use HTTPS for the browser/API outside local development.

## Quotas and scheduling

Platform administrators configure outstanding-job, concurrent CPU, concurrent memory and referenced-storage limits per project. Campaign/workflow admission is all-or-none. Repeated idempotent requests remain usable at capacity. Storage counts logical references, so two files with the same hash each consume their declared size. Dataset uploads and worker artifacts share the same project budget.

The scheduler reserves project CPU/memory budgets before reserving workers, counts all active attempts including pending cancellation, and releases capacity when an attempt reaches a terminal or retry state. Concurrent schedulers lock project rows; completion uses job/worker locks without acquiring project locks. Candidate ordering interleaves projects and uses a durable dispatch count to rotate small batches. This measures fairness by assignments; it does not guarantee equal CPU time for workloads of different lengths.

Lowering limits below existing use does not cancel running work or delete data. It stops further admission/reservation until usage fits. Disabling a project blocks client access and new assignments; existing attempts retain their leases and can finish. Global admission and worker limits still apply.

Legacy API-key mode remains available for existing trusted local deployments without projects. If projects exist and individual identity is disabled, business endpoints and readiness refuse service, preventing accidental disclosure of private data. [Federated sign-in](federated-identity.md) adds explicitly linked OIDC identities. Operational acceptance remains tracked in the product plan.

## Verification

`tests/integration/test_identity.py` covers credential hashing, durable login throttling, expiry/revocation, disabled accounts, password reset/change, per-project idempotency, nested resource isolation, role boundaries, audit actors and last-administrator protection. Real PostgreSQL tests cover simultaneous submissions and concurrent scheduler reservation. These tests are local evidence; they do not establish physical multi-host availability.
