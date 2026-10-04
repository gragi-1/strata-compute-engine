# Object storage

The v3 storage adapter supports `filesystem` (default) and S3-compatible object services. Jobs, dataset files, artifacts, worker input transfers and backup/restore use the same content-addressed abstraction. Hashes and database references preserve their v2 format.

## Configuration

Set `STRATA_STORAGE_BACKEND=s3`, `STRATA_S3_BUCKET`, `STRATA_S3_PREFIX` (default `strata/blobs/`) and `STRATA_S3_REGION` (default `us-east-1`). For compatible services, also set `STRATA_S3_ENDPOINT_URL`. Production custom endpoints require HTTPS. AWS credentials follow Boto3's standard provider chain; use a workload identity in production or supply `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optionally `AWS_SESSION_TOKEN`. Never commit credentials.

The bucket must exist. Give API/RPC and maintenance processes access only to the configured prefix: object reads/writes, listing with that prefix, and multipart upload/abort/list operations. Browsers and workers use Strata's authorized endpoints and do not receive S3 credentials. Enable encryption, versioning, service-side lifecycle policies and independent credentials according to the deployment's recovery policy.

`STRATA_ARTIFACT_ROOT` becomes a local staging/materialization directory with S3 enabled. API/RPC processes may have separate caches; the bucket and database are authoritative. Cached files are rechecked by SHA-256 and size. Missing/corrupt remote data returns `503`; it never becomes a committed dataset file or worker input. Transfers use bounded reads; uploads above 8 MiB use managed multipart transfer with two concurrent parts. The adapter verifies the source hash before upload and the retrieved bytes before serving them.

`STRATA_STORAGE_MIN_FREE_BYTES` reserves local free space (default 256 MiB). Uploads and downloads refuse insufficient capacity. This is an admission check, not a filesystem reservation: simultaneous processes can still exhaust a shared disk. Automatic cache eviction and durable retention remain separate acceptance items.

## Switching an existing installation

Stop API/RPC, schedulers and workers. Create a verified `strata-admin backup NEW_DIRECTORY` from the current backend. Restore it into an **empty database** and **empty local directory**, using the new storage settings and an empty S3 prefix, with `strata-admin restore BACKUP_DIRECTORY`. Verification checks every blob and the database dump before restore. Keep workers stopped until old sessions are expired/recovered. Test downloads and one workload before replacing the old deployment. Keep the original backup until a restore drill passes.

Backup format 1 is portable across filesystem/S3 backends. A backup downloads and verifies all referenced objects against an exported PostgreSQL snapshot. `strata-admin storage-audit` lists missing and unreferenced object hashes without deleting them. PostgreSQL and object storage are separate systems; failed SQL transactions may leave harmless unreferenced blobs. Retention must account for this and for backups before deleting anything.

## Live integration tests

`tests/integration/test_storage.py` uses an actual S3-compatible service when `STRATA_TEST_S3_ENDPOINT` is set. It verifies a multipart dataset transfer, artifact downloads, attempt-authorized inputs, rejection of corrupt remote bytes and a real PostgreSQL backup/restore. Every test creates its own bucket; backup tests create uniquely named temporary databases and remove only those resources afterward. Test-only credentials default to `strata-test` / `strata-test-secret` and can be overridden with `STRATA_TEST_S3_KEY` / `STRATA_TEST_S3_SECRET`.

CI starts a digest-pinned Versity S3 gateway and PostgreSQL for these checks. Local validation uses an isolated gateway on `127.0.0.1:59000`. This establishes compatibility with that gateway; AWS S3, another service implementation, failover and remote throughput need their own deployment evidence.
