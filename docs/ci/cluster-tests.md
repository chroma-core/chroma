### Cluster tests and shared cluster plan

This document tracks how our current Tilt-based cluster tests map to the planned shared-cluster workflow.

#### Current Tilt-dependent jobs

- **Rust k8s integration**:
  - **Workflow**: `.github/workflows/_rust-tests.yml`
  - **Job**: `test-integration`
  - **Command**: `cargo nextest run --profile ci_k8s_integration|ci_k8s_integration_slow --partition …`
- **Rust MCMR integration**:
  - **Workflow**: `.github/workflows/_rust-tests.yml`
  - **Job**: `test-mcmr-integration`
  - **Command**: `cargo nextest run --profile mcmr_k8s_integration --test-threads 1`
- **Go cluster tests**:
  - **Workflow**: `.github/workflows/_go-tests.yml`
  - **Job**: `cluster-test`
  - **Command**: `bin/cluster-test.sh bash -c 'cd go && make test'`
- **Python cluster tests (rust frontend)**:
  - **Workflow**: `.github/workflows/_python-tests.yml`
  - **Job**: `test-cluster-rust-frontend`
  - **Command**: `bin/cluster-test.sh bash -c 'python -m pytest "${{ matrix.test-glob }}" …'`

#### Planned shared-cluster workflow mapping

Once the reusable shared-cluster workflow
(`.github/workflows/_cluster-tests.yml`) is introduced, the intent is:

- **Rust k8s integration** (`test-integration`)
  - **Shared phase**: `rust_integration`
- **Rust MCMR integration** (`test-mcmr-integration`)
  - **Shared phase**: `rust_mcmr`
- **Go cluster tests** (`cluster-test`)
  - **Shared phase**: `go_cluster`
- **Python cluster tests (rust frontend)** (`test-cluster-rust-frontend`)
  - **Shared phase**: `python_cluster_frontend`

This mapping allows us to compare “old job vs. new phase” behavior and
gradually roll traffic over.

#### Allowlist for shared-cluster usage

We use a simple allowlist to control who exercises the shared-cluster workflow
once it is wired in:

- **User allowlist**: `SHARED_CLUSTER_ALLOWLIST`
  - **Source**: GitHub Actions variable `vars.SHARED_CLUSTER_ALLOWLIST`.
  - **Format**: comma-separated list of GitHub usernames, for example:
    - `alice,bob,charlie`
  - **Intended usage**:
    - In early phases, only PRs authored by users in this list will run the shared-cluster workflow.
    - When the allowlist is empty or unset, no PRs will use the shared-cluster workflow (legacy cluster jobs only).

The shared-cluster workflow will read this allowlist via inputs or environment and will be introduced in a later phase. For now, this document and the allowlist definition provide the groundwork and guardrails for the rollout.

