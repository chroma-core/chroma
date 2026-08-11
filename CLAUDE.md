# Chroma Codebase Guidelines

## Commit Message Style

Use the `[TYPE](scope): Description` format:

```
[ENH](rust-client): Add retry middleware
[BUG](rust-client): Fix connection timeout on retry
[TST](gc): Add MCMR hard delete integration test
[DOC](api): Update embeddings endpoint docs
[CHORE](deps): Bump reqwest to 0.13
```

Common types: `ENH` (feature/enhancement), `BUG` (fix), `TST` (test), `DOC`
(docs), `CHORE` (maintenance/refactor), `BLD` (build system changes).

Scope is optional but encouraged — use the component name (e.g.
`rust-client`, `gc`, `dashboard-api`).

### 50/72 Rule

- **Subject line: ≤ 50 characters.** Keeps it readable in `git log --oneline`,
  GitHub PR lists, and rebase tooling.
- **Blank line** between subject and body (required — many tools use this to
  split them).
- **Body lines: wrap at 72 characters.** Leaves room for indentation in
  80-column terminals and email patch workflows.
- **Imperative mood** in the subject: "Add login command", not "Added" or
  "Adds". Matches Git's own generated messages.
- **Body explains what and why**, not how — the diff shows how.

With the `[TYPE](scope):` prefix, aim to keep the whole subject under 50 chars.
If the scope makes that tight, drop it — the type alone is fine.

## Tilt-Backed Tests

Tests named `test_k8s_integration` depend on a local Tilt-managed Kubernetes
environment and should not be run blindly.

Before running these tests, ensure Tilt is already running and the local
services are ready. In this repo, the key readiness signals are:

- frontend healthcheck responds on `http://localhost:8000/api/v2/healthcheck`
- Spanner emulator is reachable on `localhost:9010`
- required Kubernetes pods in the `chroma` namespace are `Ready`

If a task requires running `test_k8s_integration` tests and Tilt is not
clearly ready yet, do one of these before running the tests:

- verify an existing Tilt environment is ready and then run the tests
- start or restart Tilt, wait for readiness, and only then run the tests

Do not interpret early connection failures from these tests as product bugs
until the Tilt dependency has been checked.
