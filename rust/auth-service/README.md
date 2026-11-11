# chroma-auth-service

A small single-tenant replacement for the dashboard routes consumed by the
hosted frontend. All configured API keys have the same tenant-wide permissions.
There is no dashboard database, billing integration, JWT issuance, or public
collection access. The standalone `frontend_service` binary supports this
service for authentication when the two environment variables below are set.
The frontend enforces the tenant boundary and permissions. This service does
not provide quotas.

The supported routes are `POST /api/v1/check_api_key`,
and `POST /api/v2/check_api_key`.
Each requires `x-chroma-data-plane-api-key` and a JSON body
`{"apiKey":"<tenant key>"}`. Both credentials must be valid; otherwise the service
returns 401. Optional public-collection probes never grant access.
`GET /healthz` is unauthenticated.

## Configuration

Copy `k8s/auth-service/config.example.toml` to a private location and replace the example keys.
The tenant and initial database names must contain at least three ASCII letters,
digits, underscores, or hyphens. `api_keys` is the list of valid tenant keys;
`data_plane_api_key` is a separate shared service credential.
Keys are loaded once at startup. Restart the deployment after editing the
Secret. The standalone frontend checks each request without caching keys.

```sh
cargo run -p chroma-auth-service -- --config /private/path/config.toml validate
cargo run -p chroma-auth-service -- --config /private/path/config.toml serve
```

## Tilt

`tilt up` enables authentication by default, creates the development Secret,
starts the auth service, and runs the tenant installer. The
`auth-integration-test` resource then tests the running frontend automatically.
Wait for that resource to turn green, then try:

```sh
# 401 Unauthorized
curl -i http://localhost:8000/api/v2/tenants/default_tenant

# 200 OK
curl -i -H 'x-chroma-token: tilt-test-api-key' \
  http://localhost:8000/api/v2/tenants/default_tenant

# Rerun the HTTP integration test directly
python3 k8s/test/test_auth.py
```

These are public development credentials from `k8s/test/auth-secret.yaml`.
Health checks remain public. `CHROMA_TILT_AUTH=false tilt up` restores the
unauthenticated setup for legacy tests that create arbitrary tenants. The
existing CI Tilt action uses that opt-out. The automatic local test runs with
`tilt up`; `tilt ci` has no localhost port forwards.

## Kubernetes

Build and push `docker build -f rust/Dockerfile.auth-service -t YOUR_IMAGE .`,
and replace the image in both manifests. Create the Secret from the private file
(no production credentials belong in these manifests):

```sh
kubectl -n chroma create secret generic chroma-auth-service \
  --from-file=config.toml=/private/path/config.toml \
  --dry-run=client -o yaml | kubectl -n chroma apply -f -
kubectl -n chroma apply -f k8s/auth-service/deployment.yaml
```

Configure the frontend with
`CHROMA_AUTHN_CONFIG_API_HOST=http://chroma-auth-service:8002` and
`CHROMA_AUTHN_CONFIG_DATA_PLANE_API_KEY` equal to the file's `data_plane_api_key`
(provide the environment value through a Secret). If deployed in different
namespaces, use the service's namespace-qualified DNS name. Keep the service
cluster-internal. After updating the configuration:

```sh
kubectl -n chroma rollout restart deployment/chroma-auth-service
```

Both `frontend_service` and `chroma run` retain unauthenticated behavior when
neither variable is set, and refuse to start if only one is set. With remote
auth enabled, clients send their key in `x-chroma-token`. Authentication failures
and service outages deny access; credentials are not cached. Tenantless routes
such as collection lookup by CRN are denied by this minimal provider. Embedders
can explicitly pass `chroma_frontend::auth::DashboardAuth` to the existing
frontend entrypoint.

## Install the tenant in the data plane

Once the auth service and configured frontend are ready, run:

```sh
cargo run -p chroma-auth-service -- --config /private/path/config.toml \
  install-tenant --frontend-url http://localhost:8000
```

Or edit the frontend URL in `k8s/auth-service/install-tenant.yaml` and apply that Job. The tool
uses the first configured API key with `x-chroma-token`, creates the tenant, then
creates its initial database. It accepts an existing resource only after a 409
and a successful GET verifying its name. Other failures exit nonzero. Rerunning
is safe after partial completion; delete the completed Job before submitting it
again. Tenant installation uses the supported frontend API and does not require
direct sysdb access or dashboard-issued internal tokens.

Validate locally with `cargo test -p chroma-auth-service`.
