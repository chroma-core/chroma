#!/usr/bin/env python3
"""
Chunk code data from repositories for vector database indexing.

This program reads code files from a repository, chunks them using the
chunking function from main.py, and saves the results as JSON files.
"""

import json
import os
import pathlib
from datetime import datetime
from dataclasses import asdict
from typing import Optional, List
from tree_sitter_language_pack import SupportedLanguage
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

# Import required modules
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import chunking
from modules.chunking import CodeChunk, CodeContext
from chromadb.api.types import Document

app = typer.Typer(help="Chunk code data from repositories for vector database indexing")
console = Console()

# Common code file extensions and their corresponding languages
LANGUAGE_EXTENSIONS: dict[str, SupportedLanguage] = {
    ".py": "python",
    ".js": "javascript",
    ".ts": "typescript",
    ".jsx": "javascript",
    ".tsx": "typescript",
    ".java": "java",
    ".cpp": "cpp",
    ".c": "c",
    ".php": "php",
    ".rb": "ruby",
    ".go": "go",
    ".rs": "rust",
    ".kt": "kotlin",
    ".swift": "swift",
    ".scala": "scala",
    ".sh": "bash",
    ".sql": "sql",
    ".html": "html",
    ".css": "css",
    ".json": "json",
    ".xml": "xml",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".md": "markdown",
    ".r": "r",
    ".R": "r",
    ".m": "matlab",
    ".pl": "perl",
    ".lua": "lua",
    ".vim": "vim",
    ".dockerfile": "dockerfile",
    ".Dockerfile": "dockerfile",
}


def get_language_from_extension(file_path: str) -> Optional[SupportedLanguage]:
    """Get the programming language from file extension."""
    ext = pathlib.Path(file_path).suffix.lower()
    return LANGUAGE_EXTENSIONS.get(ext)


def should_process_file(file_path: pathlib.Path) -> bool:
    """Check if a file should be processed for chunking."""
    # Skip hidden files, directories, and common non-code files
    if file_path.name.startswith("."):
        return False

    # Skip common directories that usually don't contain source code
    skip_dirs = {
        "node_modules",
        "__pycache__",
        ".git",
        ".vscode",
        ".idea",
        "build",
        "dist",
        "target",
        "bin",
        "obj",
        "vendor",
        ".next",
        ".nuxt",
        "coverage",
        "logs",
        "tmp",
    }

    if any(part in skip_dirs for part in file_path.parts):
        return False

    # Only process files with known extensions
    return get_language_from_extension(str(file_path)) is not None


def read_file_safely(file_path: pathlib.Path) -> Optional[str]:
    """Safely read a file, handling encoding issues."""
    encodings = ["utf-8", "latin-1", "cp1252"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                return f.read()
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            console.print(f"[red]Error reading {file_path}: {e}[/red]")
            return None

    console.print(f"[yellow]Could not decode {file_path} with any encoding[/yellow]")
    return None


def get_commit_hash(repo_path: pathlib.Path, commit: Optional[str]) -> str:
    """Get the commit hash either from parameter or metadata.json."""
    if commit:
        return commit

    metadata_path = repo_path / "metadata.json"
    if not metadata_path.exists():
        raise typer.BadParameter(f"Metadata file not found: {metadata_path}")

    try:
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        if "latest_commit" not in metadata:
            raise typer.BadParameter("No 'latest_commit' key found in metadata.json")

        return metadata["latest_commit"]
    except json.JSONDecodeError as e:
        raise typer.BadParameter(f"Invalid JSON in metadata file: {e}")
    except Exception as e:
        raise typer.BadParameter(f"Error reading metadata file: {e}")


def chunk_repository_files(repo_path: pathlib.Path) -> List[dict]:
    """Chunk all code files in a repository."""
    all_chunks = []
    skipped_files = 0

    # Find all code files
    code_files = []
    for file_path in repo_path.rglob("*"):
        should_process = should_process_file(file_path)
        if file_path.is_file() and should_process:
            code_files.append(file_path)
        elif file_path.is_file() and not should_process:
            skipped_files += 1

    if skipped_files > 0:
        console.print(f"[yellow]Skipped {skipped_files} files[/yellow]")

    if not code_files:
        console.print("[yellow]No code files found to process[/yellow]")
        return all_chunks

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Processing files...", total=len(code_files))

        for file_path in code_files:
            progress.update(task, description=f"Processing {file_path.name}")

            # Read file content
            content = read_file_safely(file_path)
            if content is None:
                progress.advance(task)
                continue

            # Get language
            language = get_language_from_extension(str(file_path))
            if language is None:
                progress.advance(task)
                continue

            # Create Document object (it's just a string alias)
            document: Document = content

            # Create CodeContext object
            context = CodeContext(
                language=language, file_path=str(file_path.relative_to(repo_path))
            )

            # Use the chunking function from main.py
            chunks = chunking(document, context)

            all_chunks.extend([asdict(chunk) for chunk in chunks])

            progress.advance(task)

    return all_chunks


@app.command()
def main(
    repository: str = typer.Argument(
        ..., help="GitHub repository in format 'user/reponame'"
    ),
    commit: Optional[str] = typer.Option(
        None,
        "--commit",
        "-c",
        help="Commit hash ID. If not specified, will use latest_commit from metadata.json",
    ),
):
    """
    Chunk code data from a repository for vector database indexing.

    The repository should be already downloaded in data/repos/user_reponame/
    """
    try:
        # Validate repository format
        if "/" not in repository:
            raise typer.BadParameter("Repository must be in format 'user/reponame'")

        user, repo_name = repository.split("/", 1)
        repo_dirname = f"{user}_{repo_name}"

        # Set up paths
        base_dir = pathlib.Path(__file__).parent.parent
        repo_path = base_dir / "data" / "repos" / repo_dirname

        if not repo_path.exists():
            raise typer.BadParameter(f"Repository not found: {repo_path}")

        # Get commit hash
        commit_hash = get_commit_hash(repo_path, commit)
        console.print(f"[green]Processing repository: {repository}[/green]")
        console.print(f"[green]Commit: {commit_hash}[/green]")

        # Chunk all files
        chunks = chunk_repository_files(repo_path / commit_hash)

        if not chunks:
            console.print("[yellow]No chunks generated[/yellow]")
            return

        # Create output directory
        output_dir = base_dir / "data" / "chunks" / repo_dirname
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save chunks to JSON file
        output_file = output_dir / f"{commit_hash}.json"

        output_data = {"chunks": chunks}

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        console.print(f"[green]Successfully processed {len(chunks)} chunks[/green]")
        console.print(f"[green]Saved to: {output_file}[/green]")

        metadata_file = output_dir / "metadata.json"
        try:
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
        except:
            metadata = {}

        metadata["latest_commit"] = commit_hash
        metadata["last_downloaded"] = datetime.now().isoformat()

        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        console.print(f"[green]Updated metadata to: {metadata_file}[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
