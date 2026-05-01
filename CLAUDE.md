# Chroma Codebase Guidelines

## Commit Message Style

Use the `[TYPE](scope): Description` format:

```
[ENH](foundation-cli): Add login command
[BUG](rust-client): Fix connection timeout on retry
[TST](gc): Add MCMR hard delete integration test
[DOC](api): Update embeddings endpoint docs
[CHORE](deps): Bump reqwest to 0.13
```

Common types: `ENH` (feature/enhancement), `BUG` (fix), `TST` (test), `DOC`
(docs), `CHORE` (maintenance/refactor), `BLD` (build system changes).

Scope is optional but encouraged — use the component name (e.g.
`foundation-cli`, `rust-client`, `gc`, `dashboard-api`).

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
