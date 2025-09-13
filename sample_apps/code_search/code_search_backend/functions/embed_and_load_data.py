#!/usr/bin/env python3
"""
Program to embed and load code chunks into Chroma DB.

Reads chunks from ./data/chunks/user_reponame/[commit hash ID].json,
creates embeddings using the embedding function from main.py,
and uploads them to Chroma DB.
"""

import json
import os
from pathlib import Path
from typing import Optional, List
from dataclasses import asdict

import typer
from rich.console import Console
from rich.progress import Progress, TaskID

# Import from parent directory
import sys

sys.path.append(str(Path(__file__).parent.parent))

import chromadb
from main import embedding_function
from modules.chunking import CodeChunk
from vars import MAX_CHROMA_BATCH_SIZE
from main import embedding_function

app = typer.Typer()
console = Console()


def load_metadata(repo_path: Path) -> dict:
    """Load metadata.json from the repository chunks directory."""
    metadata_path = repo_path / "metadata.json"
    if not metadata_path.exists():
        raise typer.BadParameter(f"Metadata file not found: {metadata_path}")

    with open(metadata_path, "r") as f:
        return json.load(f)


def load_chunks(chunks_file: Path) -> List[CodeChunk]:
    """Load code chunks from a JSON file."""
    if not chunks_file.exists():
        raise typer.BadParameter(f"Chunks file not found: {chunks_file}")

    with open(chunks_file, "r") as f:
        data = json.load(f)

    chunks = []
    for chunk_data in data.get("chunks", []):
        chunk = CodeChunk(
            file_path=chunk_data["file_path"],
            language=chunk_data["language"],
            start_line=chunk_data["start_line"],
            source_code=chunk_data["source_code"],
            name=chunk_data["name"],
            index_document=chunk_data.get("index_document"),
        )
        chunks.append(chunk)

    return chunks


def upload_chunks_to_chroma(
    chunks: List[CodeChunk], repo_name: str, commit_hash: str
) -> None:
    """Upload chunks to Chroma DB in batches."""
    client = chromadb.HttpClient()
    collection_name = f"{repo_name}_{commit_hash}".replace("/", "_")
    ef = embedding_function()
    try:
        client.delete_collection(collection_name)
    except Exception as e:
        pass
    collection = client.get_or_create_collection(
        name=collection_name, embedding_function=ef
    )
    console.print(f"✅ Successfully created collection: {collection_name}")

    # Prepare data for Chroma
    index_documents = []
    documents = []
    metadatas = []
    ids = []

    for i, chunk in enumerate(chunks):
        # Use index_document if available, otherwise use source_code
        documents.append(chunk.source_code)
        index_documents.append(chunk.index_document or chunk.source_code)

        # Create metadata
        metadata = {
            "file_path": chunk.file_path,
            "language": chunk.language,
            "start_line": chunk.start_line,
            "name": chunk.name,
            "repo_name": repo_name,
            "commit_hash": commit_hash,
        }
        metadatas.append(metadata)

        # Create unique ID
        chunk_id = f"{repo_name}_{commit_hash}_{i}"
        ids.append(chunk_id)

    # Upload in batches
    total_chunks = len(chunks)
    batch_size = MAX_CHROMA_BATCH_SIZE

    with Progress() as progress:
        task = progress.add_task("Uploading chunks to Chroma DB...", total=total_chunks)

        for i in range(0, total_chunks, batch_size):
            batch_end = min(i + batch_size, total_chunks)
            batch_index_documents = index_documents[i:batch_end]
            batch_documents = documents[i:batch_end]
            batch_metadatas = metadatas[i:batch_end]
            batch_ids = ids[i:batch_end]

            embeddings = ef(batch_index_documents)

            collection.add(
                documents=batch_documents,
                metadatas=batch_metadatas,
                ids=batch_ids,
                embeddings=embeddings,
            )

            progress.update(task, advance=batch_end - i)

    console.print(f"✅ Successfully uploaded {total_chunks} chunks to Chroma DB")


@app.command()
def main(
    repo_name: str = typer.Argument(
        ..., help="GitHub repository in format user/reponame"
    ),
    commit: Optional[str] = typer.Option(
        None,
        "--commit",
        help="Commit hash ID. If not provided, uses latest_commit from metadata.json",
    ),
):
    """
    Embed and load code chunks into Chroma DB.

    Reads chunks from ./data/chunks/user_reponame/[commit hash ID].json,
    creates embeddings using the embedding function, and uploads to Chroma DB.
    """

    console.print(f"🚀 Starting embed and load process for repository: {repo_name}")

    # Validate repo_name format
    if "/" not in repo_name:
        raise typer.BadParameter("Repository name must be in format 'user/reponame'")

    # Convert repo name to directory format
    repo_dir_name = repo_name.replace("/", "_")
    repo_path = Path("./data/chunks") / repo_dir_name

    if not repo_path.exists():
        raise typer.BadParameter(f"Repository chunks directory not found: {repo_path}")

    # Get commit hash
    if commit is None:
        console.print("📋 No commit specified, loading from metadata...")
        metadata = load_metadata(repo_path)
        commit = metadata.get("latest_commit")
        if not commit:
            raise typer.BadParameter("No latest_commit found in metadata.json")
        console.print(f"📋 Using commit from metadata: {commit}")
    else:
        console.print(f"📋 Using specified commit: {commit}")

    # Load chunks
    chunks_file = repo_path / f"{commit}.json"
    console.print(f"📂 Loading chunks from: {chunks_file}")
    chunks = load_chunks(chunks_file)
    console.print(f"📊 Loaded {len(chunks)} code chunks")

    # Upload to Chroma DB
    console.print("🔄 Starting upload to Chroma DB...")
    upload_chunks_to_chroma(chunks, repo_name, commit)

    console.print("🎉 Process completed successfully!")


if __name__ == "__main__":
    app()
