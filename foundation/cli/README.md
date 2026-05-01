# foundation

The Chroma Foundation CLI — team knowledge at your fingertips.

## Install

**macOS (arm64):**

```sh
gh release download --repo chroma-core/chroma \
  --pattern "foundation-cli-*_darwin_arm64.tar.gz" \
  --dir /tmp/foundation-install
tar xz -C /tmp/foundation-install -f /tmp/foundation-install/foundation-cli-*_darwin_arm64.tar.gz
mv /tmp/foundation-install/foundation /usr/local/bin/
```

Requires the [GitHub CLI](https://cli.github.com/) (`gh`). Downloads the latest
`foundation-cli` release — because the chroma repo has multiple release tags,
this is more reliable than using `/releases/latest` directly.

Or browse releases manually:
[GitHub Releases](https://github.com/chroma-core/chroma/releases?q=foundation-cli).

## Usage

```
foundation <COMMAND>

Commands:
  version     Show version information
  completion  Generate shell completion scripts
  help        Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

### Shell completions

```sh
# zsh
foundation completion zsh > ~/.zsh/completions/_foundation

# bash
mkdir -p ~/.local/share/bash-completion/completions
foundation completion bash > ~/.local/share/bash-completion/completions/foundation

# fish
foundation completion fish > ~/.config/fish/completions/foundation.fish
```

## Development

Requires Rust (stable). From the repo root:

```sh
cargo build --bin foundation --manifest-path foundation/Cargo.toml
./foundation/target/debug/foundation --help
```

Run tests:
```sh
cargo nextest run -p foundation --manifest-path foundation/Cargo.toml
```

## Links

- [Foundation Notion doc](https://app.notion.com/p/35158a6d819180218b96fac6a29eb3c8)
- [Foundation CLI spec](https://app.notion.com/p/35258a6d819180f2bdced830595c2501)
