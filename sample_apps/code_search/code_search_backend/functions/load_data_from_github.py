#!/usr/bin/env python3
"""
GitHub Repository Downloader

Downloads GitHub repositories and saves them to ./data/repos/[repo]/[commit hash ID]
"""

import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import track

app = typer.Typer(help="Download GitHub repositories to local storage")
console = Console()


def run_command(cmd: list[str], cwd: Optional[str] = None) -> tuple[int, str, str]:
    """Run a shell command and return exit code, stdout, stderr"""
    try:
        result = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, check=False
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return 1, "", str(e)


def get_latest_commit_hash(repo_url: str) -> Optional[str]:
    """Get the latest commit hash from a GitHub repository"""
    cmd = ["git", "ls-remote", repo_url, "HEAD"]
    exit_code, stdout, stderr = run_command(cmd)

    if exit_code != 0:
        console.print(f"[red]Error getting latest commit: {stderr}[/red]")
        return None

    # Parse the output to get commit hash
    lines = stdout.strip().split("\n")
    if lines and lines[0]:
        commit_hash = lines[0].split("\t")[0]
        return commit_hash

    return None


def clone_repo(
    repo_url: str, target_dir: str, commit_hash: Optional[str] = None
) -> bool:
    """Clone repository to target directory and optionally checkout specific commit"""
    try:
        # Clone the repository
        console.print(f"[blue]Cloning repository: {repo_url}[/blue]")
        cmd = ["git", "clone", repo_url, target_dir]
        exit_code, stdout, stderr = run_command(cmd)

        if exit_code != 0:
            console.print(f"[red]Error cloning repository: {stderr}[/red]")
            return False

        # Checkout specific commit if provided
        if commit_hash:
            console.print(f"[blue]Checking out commit: {commit_hash}[/blue]")
            cmd = ["git", "checkout", commit_hash]
            exit_code, stdout, stderr = run_command(cmd, cwd=target_dir)

            if exit_code != 0:
                console.print(
                    f"[red]Error checking out commit {commit_hash}: {stderr}[/red]"
                )
                return False

        # Remove .git directory to save space
        git_dir = Path(target_dir) / ".git"
        if git_dir.exists():
            shutil.rmtree(git_dir)
            console.print("[green]Removed .git directory to save space[/green]")

        return True

    except Exception as e:
        console.print(f"[red]Error during clone operation: {e}[/red]")
        return False


def parse_repo_name(repo: str) -> tuple[str, str]:
    """Parse repository name and return owner, repo_name"""
    if repo.startswith("https://github.com/"):
        repo = repo.replace("https://github.com/", "")
    elif repo.startswith("git@github.com:"):
        repo = repo.replace("git@github.com:", "")

    if repo.endswith(".git"):
        repo = repo[:-4]

    parts = repo.split("/")
    if len(parts) != 2:
        raise typer.BadParameter(
            f"Invalid repository format: {repo}. Expected format: owner/repo"
        )

    return parts[0], parts[1]


def create_or_update_metadata(
    data_dir: str, owner: str, repo_name: str, commit_hash: str
) -> None:
    """Create or update metadata.json file with latest repository information"""
    try:
        repo_base_dir = Path(data_dir) / "repos" / f"{owner}_{repo_name}"
        metadata_file = repo_base_dir / "metadata.json"

        # Load existing metadata or create new
        metadata = {}
        if metadata_file.exists():
            try:
                with open(metadata_file, "r") as f:
                    metadata = json.load(f)
            except (json.JSONDecodeError, IOError):
                console.print(
                    "[yellow]Warning: Could not read existing metadata.json, creating new one[/yellow]"
                )
                metadata = {}

        # Update metadata with latest information
        metadata.update(
            {
                "repository": f"{owner}/{repo_name}",
                "latest_commit": commit_hash,
                "last_downloaded": datetime.now().isoformat(),
                "owner": owner,
                "repo_name": repo_name,
            }
        )

        # Create directory if it doesn't exist
        repo_base_dir.mkdir(parents=True, exist_ok=True)

        # Write metadata file
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        console.print(f"[green]Updated metadata: {metadata_file}[/green]")

    except Exception as e:
        console.print(
            f"[yellow]Warning: Could not create/update metadata.json: {e}[/yellow]"
        )


@app.command()
def download(
    repo: str = typer.Argument(
        ..., help="GitHub repository (format: owner/repo or full URL)"
    ),
    commit: Optional[str] = typer.Option(
        None, "--commit", "-c", help="Specific commit hash to download"
    ),
    data_dir: str = typer.Option(
        "./data", "--data-dir", "-d", help="Base data directory"
    ),
) -> None:
    """
    Download a GitHub repository and save it to ./data/repos/[repo]/[commit hash ID]

    Examples:
        python load_data_from_github.py microsoft/vscode
        python load_data_from_github.py https://github.com/microsoft/vscode --commit abc123
    """
    try:
        # Parse repository name
        owner, repo_name = parse_repo_name(repo)
        repo_url = f"https://github.com/{owner}/{repo_name}.git"

        console.print(f"[bold]Processing repository: {owner}/{repo_name}[/bold]")

        # Get commit hash
        if commit:
            commit_hash = commit
            console.print(f"[blue]Using specified commit: {commit_hash}[/blue]")
        else:
            console.print("[blue]Getting latest commit hash...[/blue]")
            commit_hash = get_latest_commit_hash(repo_url)
            if not commit_hash:
                console.print("[red]Failed to get latest commit hash[/red]")
                raise typer.Exit(1)
            console.print(f"[green]Latest commit: {commit_hash}[/green]")

        # Create target directory structure
        repo_dir = Path(data_dir) / "repos" / f"{owner}_{repo_name}" / commit_hash
        repo_dir.mkdir(parents=True, exist_ok=True)

        # Check if already exists
        if repo_dir.exists() and any(repo_dir.iterdir()):
            console.print(f"[yellow]Repository already exists at: {repo_dir}[/yellow]")
            overwrite = typer.confirm("Overwrite existing repository?")
            if not overwrite:
                console.print("[blue]Skipping download[/blue]")
                return
            else:
                shutil.rmtree(repo_dir)
                repo_dir.mkdir(parents=True, exist_ok=True)

        # Clone repository
        success = clone_repo(repo_url, str(repo_dir), commit_hash)

        if success:
            console.print(
                f"[bold green]✓ Successfully downloaded repository to: {repo_dir}[/bold green]"
            )

            # Create or update metadata.json
            create_or_update_metadata(data_dir, owner, repo_name, commit_hash)

            # Show summary
            file_count = sum(1 for _ in Path(repo_dir).rglob("*") if _.is_file())
            dir_size = sum(
                f.stat().st_size for f in Path(repo_dir).rglob("*") if f.is_file()
            )
            dir_size_mb = dir_size / (1024 * 1024)

            console.print(f"[dim]Files: {file_count}, Size: {dir_size_mb:.1f} MB[/dim]")
        else:
            console.print("[red]Failed to download repository[/red]")
            raise typer.Exit(1)

    except typer.BadParameter as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
