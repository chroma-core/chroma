#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "GitPython",
#   "rich",
#   "InquirerPy",
#   "packaging",
#   "tomlkit"
# ]
# ///
"""
Chroma Release Script

Usage:
    uv run release.py [python|js|rust|cli]
"""

import argparse
import json
import re
import tomlkit
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional, cast

from git import Repo
from InquirerPy import inquirer
from packaging import version as semver
from rich import print
from rich.console import Console

MAIN_BRANCH = "main"
console = Console()


def log_step(msg: str) -> None:
    console.print(f"  [dim]→[/dim] {msg}")


def log_success(msg: str) -> None:
    console.print(f"  [green]✓[/green] {msg}")


class ReleaserError(Exception):
    pass


# =============================================================================
# File Abstractions
# =============================================================================


class VersionFile(ABC):
    """
    A file containing one or more version strings.

    Supports reading/writing versions by key. The meaning of 'key' depends on
    the file type (regex pattern name, JSON path, TOML path).
    """

    def __init__(self, path: Path, name: str = "", default_key: str = "version"):
        self.path = path
        self.name = name or path.name
        self.default_key = default_key
        self._dirty = False
        self._load()

    @abstractmethod
    def _load(self) -> None:
        """Load and parse the file."""
        pass

    @abstractmethod
    def _serialize(self) -> str:
        """Serialize back to string."""
        pass

    @abstractmethod
    def get(self, key: str) -> str:
        """Get version at the given key."""
        pass

    @abstractmethod
    def set(self, key: str, version: str) -> None:
        """Set version at the given key."""
        pass

    @property
    def version(self) -> str:
        """Get the default version."""
        return self.get(self.default_key)

    @version.setter
    def version(self, value: str) -> None:
        """Set the default version."""
        self.set(self.default_key, value)

    def save(self) -> bool:
        """Save if modified. Returns True if saved."""
        if self._dirty:
            self.path.write_text(self._serialize(), encoding="utf-8")
            self._dirty = False
            return True
        return False


@dataclass
class RegexVersionFile(VersionFile):
    """
    A file where versions are found/updated via regex.

    The 'key' parameter is ignored - these files have a single version pattern.
    """

    path: Path
    pattern: str
    name: str = ""
    default_key: str = "version"
    _content: str = field(default="", init=False, repr=False)

    def __post_init__(self):
        self.name = self.name or self.path.name
        self._compiled = re.compile(self.pattern)
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            raise ReleaserError(f"File not found: {self.path}")
        self._content = self.path.read_text(encoding="utf-8")

    def _serialize(self) -> str:
        return self._content

    def get(self, key: str = "version") -> str:
        match = self._compiled.search(self._content)
        if not match:
            raise ReleaserError(f"Version pattern not found in {self.name}")
        return match.group(1)

    def set(self, key: str, version: str) -> None:
        new_content, count = self._compiled.subn(
            lambda m: m.group(0).replace(m.group(1), version),
            self._content,
            count=1,
        )
        if count != 1:
            raise ReleaserError(f"Expected 1 match in {self.name}, found {count}")
        self._content = new_content
        self._dirty = True


@dataclass
class JSONVersionFile(VersionFile):
    """
    A JSON file with versions at dot-separated paths.

    Key format: "path.to.version" -> data["path"]["to"]["version"]
    """

    path: Path
    name: str = ""
    default_key: str = "version"
    _data: dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        self.name = self.name or self.path.name
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            raise ReleaserError(f"File not found: {self.path}")
        self._data = json.loads(self.path.read_text(encoding="utf-8"))

    def _serialize(self) -> str:
        return json.dumps(self._data, indent=2) + "\n"

    def _navigate(self, keys: list[str], create: bool = False) -> tuple[Any, str]:
        """Navigate to parent node and return (parent, final_key)."""
        node = self._data
        for k in keys[:-1]:
            if k not in node:
                if create:
                    node[k] = {}
                else:
                    raise ReleaserError(f"Key '{k}' not found in {self.name}")
            node = node[k]
        return node, keys[-1]

    def get(self, key: str) -> str:
        parent, final = self._navigate(key.split("."))
        return parent[final]

    def set(self, key: str, version: str) -> None:
        parent, final = self._navigate(key.split("."))
        parent[final] = version
        self._dirty = True


@dataclass
class TOMLVersionFile(VersionFile):
    """
    A TOML file with versions at dot-separated paths.

    Uses tomlkit to preserve formatting and comments.
    Key format: "package.version" -> doc["package"]["version"]
    """

    path: Path
    name: str = ""
    default_key: str = "package.version"
    _doc: tomlkit.TOMLDocument = field(default=None, init=False, repr=False)

    def __post_init__(self):
        self.name = self.name or self.path.name
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            raise ReleaserError(f"File not found: {self.path}")
        self._doc = tomlkit.parse(self.path.read_text(encoding="utf-8"))

    def _serialize(self) -> str:
        return tomlkit.dumps(self._doc)

    def _navigate(self, keys: list[str]) -> tuple[Any, str]:
        """Navigate to parent node and return (parent, final_key)."""
        node = self._doc
        for k in keys[:-1]:
            if k not in node:
                raise ReleaserError(f"Key '{k}' not found in {self.name}")
            node = node[k]
        return node, keys[-1]

    def get(self, key: str) -> str:
        parent, final = self._navigate(key.split("."))
        value = parent[final]
        # Handle inline tables like { path = "...", version = "..." }
        if isinstance(value, dict) and "version" in value:
            return value["version"]
        return value

    def set(self, key: str, version: str) -> None:
        parent, final = self._navigate(key.split("."))
        value = parent[final]
        if isinstance(value, dict) and "version" in value:
            value["version"] = version
        else:
            parent[final] = version
        self._dirty = True


# =============================================================================
# File Manager - Caching & Batch Operations
# =============================================================================


class FileManager:
    """Manages file loading with caching and batch saves."""

    def __init__(self):
        self._cache: dict[Path, VersionFile] = {}

    def load(self, file: VersionFile) -> VersionFile:
        """Load a file, using cache if already loaded."""
        if file.path not in self._cache:
            self._cache[file.path] = file
        return self._cache[file.path]

    def save_all(self) -> list[Path]:
        """Save all modified files. Returns list of saved paths."""
        saved = []
        for path, file in self._cache.items():
            if file.save():
                saved.append(path)
        return saved

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()


# =============================================================================
# File Definitions
# =============================================================================

# Factory functions for lazy instantiation


def python_init():
    return RegexVersionFile(
        path=Path("chromadb/__init__.py"),
        pattern=r'__version__\s*=\s*["\']([^"\']+)["\']',
        name="chromadb/__init__.py",
    )


def aws_cloudformation():
    return JSONVersionFile(
        path=Path("deployments/aws/chroma.cf.json"),
        name="AWS CloudFormation",
        default_key="Parameters.ChromaVersion.Default",
    )


def azure_main_tf():
    return RegexVersionFile(
        path=Path("deployments/azure/main.tf"),
        pattern=r'variable\s+"chroma_version"\s*{\s*[^}]*?default\s*=\s*"([^"]+)"',
        name="Azure main.tf",
    )


def gcp_main_tf():
    return RegexVersionFile(
        path=Path("deployments/gcp/main.tf"),
        pattern=r'variable\s+"chroma_version"\s*{\s*[^}]*?default\s*=\s*"([^"]+)"',
        name="GCP main.tf",
    )


def chromadb_package_json():
    return JSONVersionFile(
        path=Path("clients/new-js/packages/chromadb/package.json"),
        name="chromadb package.json",
    )


def cargo_chroma():
    return TOMLVersionFile(path=Path("rust/chroma/Cargo.toml"), name="chroma crate")


def cargo_error():
    return TOMLVersionFile(path=Path("rust/error/Cargo.toml"), name="chroma-error crate")


def cargo_types():
    return TOMLVersionFile(path=Path("rust/types/Cargo.toml"), name="chroma-types crate")


def cargo_api_types():
    return TOMLVersionFile(
        path=Path("rust/api-types/Cargo.toml"), name="chroma-api-types crate"
    )


def cargo_root():
    return TOMLVersionFile(path=Path("Cargo.toml"), name="root Cargo.toml")


def cargo_cli():
    return TOMLVersionFile(path=Path("rust/cli/Cargo.toml"), name="CLI Cargo.toml")


def cli_lib_rs():
    return RegexVersionFile(
        path=Path("rust/cli/src/lib.rs"),
        pattern=r'#\[command\(version\s*=\s*"([^"]+)"\)\]',
        name="CLI lib.rs",
    )


def cli_install_sh():
    return RegexVersionFile(
        path=Path("rust/cli/install/install.sh"),
        pattern=r'RELEASE="cli-([^"]+)"',
        name="CLI install.sh",
    )


def cli_install_ps1():
    return RegexVersionFile(
        path=Path("rust/cli/install/install.ps1"),
        pattern=r'\$release\s*=\s*"cli-([^"]+)"',
        name="CLI install.ps1",
    )


def js_bindings_package_json():
    return JSONVersionFile(
        path=Path("rust/js_bindings/package.json"),
        name="JS bindings package.json",
    )


def js_bindings_platform(platform: str):
    return JSONVersionFile(
        path=Path(f"rust/js_bindings/npm/{platform}/package.json"),
        name=f"JS bindings {platform}",
    )


JS_BINDINGS_PLATFORMS = [
    "darwin-arm64",
    "darwin-x64",
    "linux-arm64-gnu",
    "linux-x64-gnu",
    "win32-arm64-msvc",
    "win32-x64-msvc",
]

# File groups as lists of factory functions
PYTHON_FILES = [
    python_init,
    aws_cloudformation,
    azure_main_tf,
    gcp_main_tf,
]

JS_FILES = [chromadb_package_json]

RUST_CRATES = [cargo_error, cargo_types, cargo_api_types, cargo_chroma]

RUST_WORKSPACE_DEPS = [
    "workspace.dependencies.chroma-error",
    "workspace.dependencies.chroma-types",
    "workspace.dependencies.chroma-api-types",
    "workspace.dependencies.chroma",
]

CLI_FILES = [cargo_cli, cli_lib_rs, cli_install_sh, cli_install_ps1]

JS_BINDINGS_OPTIONAL_DEPS = [
    "optionalDependencies.chromadb-js-bindings-darwin-arm64",
    "optionalDependencies.chromadb-js-bindings-darwin-x64",
    "optionalDependencies.chromadb-js-bindings-linux-arm64-gnu",
    "optionalDependencies.chromadb-js-bindings-linux-x64-gnu",
    "optionalDependencies.chromadb-js-bindings-win32-x64-msvc",
]


# =============================================================================
# Git Operations
# =============================================================================


class GitOperations:
    """Handles all git-related operations."""

    def __init__(self):
        if not Path(".git").exists():
            raise ReleaserError("Not in a git repository root")

        self.repo = Repo(".")
        self.created_branch: Optional[str] = None

    def ensure_clean_main(self) -> None:
        """Verify we're on main with a clean working tree."""
        if self.repo.active_branch.name != MAIN_BRANCH:
            raise ReleaserError(f"Must be on '{MAIN_BRANCH}' branch")
        if self.repo.is_dirty():
            raise ReleaserError("Working tree is not clean")
        if self.repo.untracked_files:
            console.print("[yellow]Warning:[/yellow] Untracked files present")

    def create_branch(self, name: str) -> None:
        """Create and checkout a new branch."""
        log_step(f"Creating branch: {name}")
        try:
            branch = self.repo.create_head(name)
            branch.checkout()
            self.created_branch = name
            log_success(f"Created branch: {name}")
        except Exception as e:
            raise ReleaserError(f"Failed to create branch: {e}")

    def commit(self, files: list[Path], message: str) -> None:
        """Stage files and commit."""
        log_step("Staging changes")
        self.repo.index.add([str(f) for f in files])
        log_step("Committing")
        self.repo.index.commit(message)
        log_success(f"Committed: {message}")

    def cleanup(self) -> None:
        """Reset to main and delete created branch."""
        console.print("\n[yellow]Cleaning up...[/yellow]")
        try:
            self.repo.head.reset(index=True, working_tree=True)
            if self.repo.active_branch.name != MAIN_BRANCH:
                self.repo.heads[MAIN_BRANCH].checkout()
            if self.created_branch and self.created_branch in self.repo.heads:
                self.repo.delete_head(self.created_branch, force=True)
                log_success(f"Deleted branch: {self.created_branch}")
        except Exception as e:
            console.print(f"[red]Cleanup failed:[/red] {e}")


# =============================================================================
# Release Logic
# =============================================================================


class ReleasePath(Enum):
    PYTHON = "python"
    JS = "js"
    RUST = "rust"
    CLI = "cli"


def increment_patch(ver: str) -> str:
    """Calculate next patch version."""
    v = semver.parse(ver)
    major, minor, patch = v.release
    return f"{major}.{minor}.{patch + 1}"


def prompt_version(current: str, name: str) -> str:
    """Prompt for new version with next patch as default."""
    default = increment_patch(current)
    return inquirer.text(
        message=f"New {name} version (current: {current})",
        default=default,
    ).execute()


class Releaser:
    """Orchestrates the release process."""

    def __init__(self):
        self.git = GitOperations()
        self.files = FileManager()

    def update_files(
            self, factories: list[Callable[[], VersionFile]], version: str
    ) -> None:
        """Load files from factories and update their default version."""
        for factory in factories:
            f = self.files.load(factory())
            log_step(f"Updating {f.name} ({f.version} → {version})")
            f.version = version
            log_success(f"Updated {f.name}")

    def finalize(self, branch: str, message: str, label: str) -> None:
        """Save files, commit, and print next steps."""
        saved = self.files.save_all()
        self.git.commit(saved, message)

        console.print("\n[bold green]Release branch ready![/bold green]\n")
        console.print("Next steps:")
        console.print(f"  1. [cyan]git push origin {branch}[/cyan]")
        console.print(f"  2. Create PR with label: [cyan]{label}[/cyan]")
        console.print("  3. Get review and merge")

    def release_python(self) -> None:
        console.print("\n[bold blue]Python Release[/bold blue]\n")

        init_file = self.files.load(python_init())
        version = prompt_version(init_file.version, "Python")

        branch = f"release/python-{version}"
        self.git.create_branch(branch)

        print()
        self.update_files(PYTHON_FILES, version)
        self.finalize(branch, f"[RELEASE] Python {version}", "release-python")

    def release_js(self) -> None:
        console.print("\n[bold yellow]JS/TS Release[/bold yellow]\n")

        pkg = self.files.load(chromadb_package_json())
        version = prompt_version(pkg.version, "JS")

        branch = f"release/js-{version}"
        self.git.create_branch(branch)

        print()
        self.update_files(JS_FILES, version)
        self.finalize(branch, f"[RELEASE] JS {version}", "release-js")

    def release_rust(self) -> None:
        console.print("\n[bold red]Rust Release[/bold red]\n")

        chroma = self.files.load(cargo_chroma())
        version = prompt_version(chroma.version, "Rust")

        branch = f"release/rust-{version}"
        self.git.create_branch(branch)

        print()
        self.update_files(RUST_CRATES, version)

        # Update workspace dependencies in root Cargo.toml
        root = self.files.load(cargo_root())
        log_step(f"Updating workspace dependencies in {root.name}")
        for dep_key in RUST_WORKSPACE_DEPS:
            root.set(dep_key, version)
        log_success(f"Updated {root.name}")

        self.finalize(branch, f"[RELEASE] Rust {version}", "release-rust")

    def release_cli(self) -> None:
        console.print("\n[bold magenta]CLI Release[/bold magenta]\n")

        # Prompt for all versions upfront (CLI → JS Bindings → Python → JS)
        cli = self.files.load(cargo_cli())
        cli_version = prompt_version(cli.version, "CLI")

        bindings = self.files.load(js_bindings_package_json())
        bindings_version = prompt_version(bindings.version, "JS Bindings")

        py_init = self.files.load(python_init())
        python_version = prompt_version(py_init.version, "Python")

        js_pkg = self.files.load(chromadb_package_json())
        js_version = prompt_version(js_pkg.version, "JS")

        branch = f"release/cli-{cli_version}-python-{python_version}-js-{js_version}"
        self.git.create_branch(branch)

        # CLI files
        console.print("\n[bold]CLI files[/bold]")
        self.update_files(CLI_FILES, cli_version)

        # JS Bindings files
        console.print("\n[bold]JS Bindings files[/bold]")
        self.update_files([js_bindings_package_json], bindings_version)
        for platform in JS_BINDINGS_PLATFORMS:
            factory = lambda p=platform: js_bindings_platform(p)
            self.update_files([factory], bindings_version)

        # Update optionalDependencies in chromadb package.json
        log_step("Updating optionalDependencies")
        for dep_key in JS_BINDINGS_OPTIONAL_DEPS:
            js_pkg.set(dep_key, f"^{bindings_version}")
        log_success("Updated optionalDependencies")

        # Python files
        console.print("\n[bold]Python files[/bold]")
        self.update_files(PYTHON_FILES, python_version)

        # JS files (already loaded, just update version)
        console.print("\n[bold]JS files[/bold]")
        log_step(f"Updating {js_pkg.name} ({js_pkg.version} → {js_version})")
        js_pkg.version = js_version
        log_success(f"Updated {js_pkg.name}")

        message = f"[RELEASE] CLI {cli_version}, Python {python_version}, JS {js_version}"
        self.finalize(branch, message, "release-all")

    def run(self, path: Optional[ReleasePath] = None) -> None:
        self.git.ensure_clean_main()

        if path is None:
            path = cast(
                ReleasePath,
                inquirer.select(
                    message="Select release type",
                    choices=[
                        {"name": "CLI (includes clients)", "value": ReleasePath.CLI},
                        {"name": "Python", "value": ReleasePath.PYTHON},
                        {"name": "JS/TS", "value": ReleasePath.JS},
                        {"name": "Rust", "value": ReleasePath.RUST},
                    ],
                ).execute(),
            )

        {
            ReleasePath.PYTHON: self.release_python,
            ReleasePath.JS: self.release_js,
            ReleasePath.RUST: self.release_rust,
            ReleasePath.CLI: self.release_cli,
        }[path]()


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(description="Chroma Release Script")
    parser.add_argument(
        "path",
        nargs="?",
        choices=["python", "js", "rust", "cli"],
        help="Release path",
    )
    args = parser.parse_args()

    releaser: Optional[Releaser] = None
    try:
        console.print("\n[bold]🚀 Chroma Release Script[/bold]\n")
        releaser = Releaser()
        releaser.run(ReleasePath(args.path) if args.path else None)

    except KeyboardInterrupt:
        console.print("\n[yellow]Cancelled[/yellow]")
        if releaser:
            releaser.cleanup()
    except ReleaserError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        if releaser:
            releaser.git.cleanup()
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        if releaser:
            releaser.git.cleanup()
        raise


if __name__ == "__main__":
    main()