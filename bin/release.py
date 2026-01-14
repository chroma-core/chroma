#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "GitPython",
#   "rich",
#   "InquirerPy",
#   "packaging"
# ]
# ///

import argparse
import json
import re
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional, cast, Callable, Any
from git import Repo
from rich import print
from rich.console import Console
from InquirerPy import inquirer
from packaging import version


MAIN_BRANCH = "main"
console = Console()


def log_step(message: str) -> None:
    console.print(f"  [dim]→[/dim] {message}")


def log_success(message: str) -> None:
    console.print(f"  [green]✓[/green] {message}")


class ReleaserError(Exception):
    def __init__(self, message: str):
        self.message = f"[bold red]Error:[/bold red] {message}"


class VersionFile(ABC):
    def __init__(self, name: str, path: str):
        self.name = name
        self.path = Path(path)
        self.current_version = self._load_current_version()

    @abstractmethod
    def _load_current_version(self) -> str:
        pass

    @abstractmethod
    def update_version(self, new_version: str) -> None:
        pass


class GeneralVersionFile(VersionFile):
    content: str
    version_pattern: re.Pattern[str]

    def __init__(self, name: str, path: str, version_pattern: str):
        self.version_pattern = re.compile(version_pattern)

        file_path = Path(path)
        if not file_path.exists():
            raise ReleaserError(f"{name} not found at {path}")

        try:
            self.content = file_path.read_text(encoding="utf-8")
        except Exception:
            raise ReleaserError(f"Failed to read {name} at {path}")

        super().__init__(name, path)

    def _load_current_version(self) -> str:
        match = self.version_pattern.search(self.content)
        if not match:
            raise ReleaserError(f"Failed to get current {self.name} version")
        return match.group(1)

    def update_version(self, new_version: str) -> None:
        new_content, count = self.version_pattern.subn(
            lambda m: m.group(0).replace(m.group(1), new_version),
            self.content,
            count=1,
        )

        if count != 1:
            raise ReleaserError(
                f"Expected to update exactly one version string in {self.name}, found {count}"
            )

        try:
            self.path.write_text(new_content, encoding="utf-8")
        except Exception:
            raise ReleaserError(f"Failed to write updated version to {self.path}")

        self.content = new_content
        self.current_version = new_version


class JSONVersionFile(VersionFile):
    content: dict[str, Any]
    version_path: list[str]

    def __init__(self, name: str, path: str, version_path: list[str]):
        self.version_path = version_path

        file_path = Path(path)
        if not file_path.exists():
            raise ReleaserError(f"{name} not found at {path}")

        try:
            self.text = file_path.read_text(encoding="utf-8")
            self.content = json.loads(self.text)
        except Exception:
            raise ReleaserError(f"Failed to read {name} at {path}")

        super().__init__(name, path)

    def _load_current_version(self) -> str:
        node: Any = self.content
        for key in self.version_path:
            if key not in node:
                raise ReleaserError(f"Version path key {key} not found in {self.name} JSON file")
            node = node[key]
        if not isinstance(node, str):
            raise ReleaserError(f"Invalid version path for {self.name}")
        return node


    def update_version(self, new_version: str) -> None:
        node: Any = self.content
        for key in self.version_path[:-1]:
            if key not in node:
                raise ReleaserError(f"Version path key {key} not found in {self.name} JSON file")
            node = node[key]

        if not isinstance(node[self.version_path[-1]], str):
            raise ReleaserError(f"Invalid version path for {self.name}")

        node[self.version_path[-1]] = new_version

        try:
            self.path.write_text(json.dumps(self.content, indent=2) + "\n", encoding="utf-8")
        except Exception:
            raise ReleaserError(f"Failed to write updated version to {self.path}")

        self.current_version = new_version


class ReleasePath(Enum):
    CLI = "cli"
    PYTHON = "python"
    JS = "js"
    RUST = "rust"


RELEASE_PATHS = [
    {"name": "CLI (including clients)", "value": ReleasePath.CLI},
    {"name": "Python Client", "value": ReleasePath.PYTHON},
    {"name": "JS/TS", "value": ReleasePath.JS},
    {"name": "Rust", "value": ReleasePath.RUST}
]


class FilesRegistry:
    python_init = GeneralVersionFile(
        "__init__.py",
        "./chromadb/__init__.py",
        r'__version__\s*=\s*["\']([^"\']+)["\']'
    )

    cloudflare_template = JSONVersionFile(
        "Cloudflare Template",
        "deployments/aws/chroma.cf.json",
        ["Parameters", "ChromaVersion", "Default"],
    )

    azure_template_vars = GeneralVersionFile(
        "Azure Template Variables",
        "deployments/azure/chroma.tfvars.tf",
        r'chroma_version\s*=\s*["\']([^"\']+)["\']'
    )

    azure_template = GeneralVersionFile(
        "Azure Template",
        "deployments/azure/main.tf",
        r'variable\s+"chroma_version"\s*{\s*[^}]*?default\s*=\s*"([^"]+)"'
    )

    gcp_template_vars = GeneralVersionFile(
        "GCP Template Variables",
        "deployments/gcp/chroma.tfvars",
        r'chroma_version\s*=\s*["\']([^"\']+)["\']'
    )

    gcp_template = GeneralVersionFile(
        "GCP Template",
        "deployments/gcp/main.tf",
        r'variable\s+"chroma_version"\s*{\s*[^}]*?default\s*=\s*"([^"]+)"'
    )

    chromadb_package_json = JSONVersionFile(
        "chromadb/package.json",
        "clients/new-js/packages/chromadb/package.json",
        ["version"]
    )

    @staticmethod
    def deployment_files() -> list[VersionFile]:
        return [
            FilesRegistry.cloudflare_template,
            FilesRegistry.azure_template_vars,
            FilesRegistry.azure_template,
            FilesRegistry.gcp_template_vars,
            FilesRegistry.gcp_template,
        ]


def increment_patch_version(current_version: str) -> str:
    v = version.parse(current_version)
    major, minor, patch = v.release
    new_version = f"{major}.{minor}.{patch + 1}"
    return new_version

def prompt_new_version(version_file: VersionFile, name: str) -> str:
    current_version = version_file.current_version
    suggested_version = increment_patch_version(current_version)

    new_version = inquirer.text(
        message=f"Enter new release version for the {name} (current version: {current_version})",
        default=suggested_version,
    ).execute()

    return new_version


def get_branch_name(release_path: ReleasePath, new_version: str) -> str:
    prefixes = {
        ReleasePath.PYTHON: "python",
        ReleasePath.JS: "js",
        ReleasePath.CLI: "cli",
        ReleasePath.RUST: "rust"
    }

    return f"release/{prefixes[release_path]}-{new_version}"


class ChromaReleaser:
    repo: Repo
    release_branch_name: Optional[str] = None

    def __init__(self):
        cwd = Path.cwd()
        if not (cwd / ".git").exists():
            raise ReleaserError("Run this script from the root of the Chroma repo")

        self.repo = Repo(".")
        if self.repo.active_branch.name != MAIN_BRANCH:
            raise ReleaserError("Check out the main branch to proceed")
        if self.repo.is_dirty():
            raise ReleaserError("Make sure that the repo is in a clean state")
        if len(self.repo.untracked_files) > 0:
            print("[bold yellow]Warning:[/bold yellow] Repo has untracked files")

    def cleanup(self) -> None:
        """Restore repo to clean state on main branch."""
        if self.release_branch_name is None:
            return

        console.print(f"\n[bold yellow]Cleaning up...[/bold yellow]")
        try:
            # Discard any uncommitted changes
            self.repo.head.reset(index=True, working_tree=True)
            # Checkout main
            self.repo.heads[MAIN_BRANCH].checkout()
            log_success(f"Checked out {MAIN_BRANCH}")
            # Delete the release branch
            self.repo.delete_head(self.release_branch_name, force=True)
            log_success(f"Deleted branch {self.release_branch_name}")
        except Exception as e:
            console.print(f"[bold red]Cleanup failed:[/bold red] {e}")
            console.print(f"You may need to manually run: git checkout {MAIN_BRANCH} && git branch -D {self.release_branch_name}")

    def create_release_branch(self, name: str) -> None:
        log_step(f"Creating branch {name}")
        try:
            release_branch = self.repo.create_head(name)
        except Exception as e:
            raise ReleaserError(f"Failed to create branch {name}: {e}")

        try:
            release_branch.checkout()
        except Exception as e:
            # Try to delete the branch we just created
            try:
                self.repo.delete_head(name, force=True)
            except Exception:
                pass
            raise ReleaserError(f"Failed to checkout branch {name}: {e}")

        self.release_branch_name = name
        log_success(f"Created and checked out branch {name}")

    def release_cli(self) -> None:
        pass

    def release_rust(self) -> None:
        pass

    def release_js(self) -> None:
        new_version = prompt_new_version(FilesRegistry.chromadb_package_json, "JS/TS client")
        branch_name = get_branch_name(ReleasePath.JS, new_version)
        self.create_release_branch(branch_name)

        update_files = [
            FilesRegistry.chromadb_package_json
        ]

        for file in update_files:
            log_step(f"Updating {file.name} ({file.current_version} → {new_version})")
            file.update_version(new_version)
            log_success(f"Updated {file.name}")

        log_step("Staging changes")
        self.repo.index.add([file.path for file in update_files])
        log_step("Creating commit")
        self.repo.index.commit(f"[RELEASE] JS/TS {new_version}")
        log_success("Committed changes")

        print()
        print("[bold green]Release ready![/bold green]")
        print(f"1. Push the release branch: git push origin {branch_name}")
        print("2. Label the PR with the 'release-js' label")
        print("3. Make sure the PR is approved, merged, and green on main")

    def release_python(self) -> None:
        new_version = prompt_new_version(FilesRegistry.python_init, "Python client")
        branch_name = get_branch_name(ReleasePath.PYTHON, new_version)
        self.create_release_branch(branch_name)

        update_files = [
            FilesRegistry.python_init,
            *FilesRegistry.deployment_files()
        ]

        for file in update_files:
            log_step(f"Updating {file.name} ({file.current_version} → {new_version})")
            file.update_version(new_version)
            log_success(f"Updated {file.name}")

        log_step("Staging changes")
        self.repo.index.add([file.path for file in update_files])
        log_step("Creating commit")
        self.repo.index.commit(f"[RELEASE] Python {new_version}")
        log_success("Committed changes")

        print()
        print("[bold green]Release ready![/bold green]")
        print(f"1. Push the release branch: git push origin {branch_name}")
        print("2. Label the PR with the 'release-python' label")
        print("3. Make sure the PR is approved, merged, and green on main")

    def run(self, release_path: Optional[ReleasePath] = None) -> None:
        if release_path is None:
            selected = cast(ReleasePath, inquirer.select(
                message="Select release path:",
                choices=RELEASE_PATHS,
            ).execute())

            release_path = selected

        releasers: dict[ReleasePath, Callable[[], None]] = {
            ReleasePath.CLI: self.release_cli,
            ReleasePath.PYTHON: self.release_python,
            ReleasePath.JS: self.release_js,
            ReleasePath.RUST: self.release_rust
        }

        releasers[release_path]()

def parse_args() -> Optional[ReleasePath]:
    parser = argparse.ArgumentParser(description="Chroma Release Wizard")
    parser.add_argument(
        "release_path",
        choices=["cli", "js", "python", "rust"],
        nargs="?",
        help="The release path: Python, JS, Rust clients, or CLI (including all clients)",
    )
    args = parser.parse_args()
    return ReleasePath(args.release_path) if args.release_path else None

def main() -> None:
    releaser: Optional[ChromaReleaser] = None
    try:
        release_path = parse_args()
        releaser = ChromaReleaser()
        releaser.run(release_path)
    except ReleaserError as e:
        print(e.message)
        if releaser:
            releaser.cleanup()
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        if releaser:
            releaser.cleanup()

if __name__ == "__main__":
    main()