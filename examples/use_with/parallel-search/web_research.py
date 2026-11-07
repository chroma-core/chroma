"""Save web excerpts through MCP, then retrieve them with local Chroma."""

import argparse
import asyncio
import hashlib
import json
import sys
import uuid
from typing import Any

import chromadb
from chromadb.api.models.Collection import Collection
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import CallToolResult


MCP_URL = "https://search.parallel.ai/mcp"
USER_AGENT = "chroma-parallel-search-example/1.0"
COLLECTION = "web_research"


def tool_data(result: CallToolResult) -> dict[str, Any]:
    if result.isError:
        raise RuntimeError(f"MCP tool failed: {result.content}")
    structured: dict[str, Any] | None = result.structuredContent
    if structured is not None:
        return structured
    # Older MCP servers may return JSON in a text content block.
    for block in result.content:
        if block.type == "text":
            data: dict[str, Any] = json.loads(block.text)
            return data
    raise RuntimeError("MCP tool returned no JSON data")


async def retrieve(args: argparse.Namespace) -> dict[str, Any]:
    session_id = str(uuid.uuid4())
    if args.command == "search":
        name = "web_search"
        arguments: dict[str, Any] = {
            "objective": args.objective,
            "search_queries": args.search_query,
            "session_id": session_id,
        }
    else:
        name = "web_fetch"
        arguments = {"urls": args.urls, "session_id": session_id}
        if args.objective:
            arguments["objective"] = args.objective

    async with streamablehttp_client(
        MCP_URL, headers={"User-Agent": USER_AGENT}, timeout=30, sse_read_timeout=120
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            if name not in {tool.name for tool in tools.tools}:
                raise RuntimeError(f"MCP server does not offer {name}")
            return tool_data(await session.call_tool(name, arguments=arguments))


def save(collection: Collection, data: dict[str, Any]) -> None:
    chunks = {}
    for page in data["results"]:
        for excerpt in page["excerpts"]:
            for start in range(0, len(excerpt), 1000):
                text = excerpt[start : start + 1000].strip()
                if not text:
                    continue
                key = hashlib.sha256((page["url"] + "\n" + text).encode()).hexdigest()
                chunks[key] = (
                    text,
                    {"url": page["url"], "title": page.get("title") or page["url"]},
                )
    for error in data.get("errors", []):
        print(f"Could not fetch {error['url']}: {error['error_type']}", file=sys.stderr)
    for warning in data.get("warnings") or []:
        print(warning["message"], file=sys.stderr)
    if not chunks:
        raise RuntimeError("No nonempty excerpts returned; nothing was saved")
    collection.upsert(
        ids=list(chunks),
        documents=[chunk[0] for chunk in chunks.values()],
        metadatas=[chunk[1] for chunk in chunks.values()],
    )
    print(
        f"Saved {len(chunks)} chunks. Collection contains {collection.count()} chunks."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", default=".chroma-parallel")
    commands = parser.add_subparsers(dest="command", required=True)
    search = commands.add_parser("search", help="Search the web and save excerpts")
    search.add_argument("objective")
    search.add_argument("--search-query", action="append", required=True)
    fetch = commands.add_parser("fetch", help="Fetch known URLs and save excerpts")
    fetch.add_argument("urls", nargs="+")
    fetch.add_argument("--objective")
    query = commands.add_parser("query", help="Retrieve locally saved excerpts")
    query.add_argument("text")
    args = parser.parse_args()
    if args.command == "fetch":
        if len(args.urls) > 20:
            parser.error("web_fetch accepts at most 20 URLs")
        if args.objective and len(args.objective) > 200:
            parser.error("web_fetch objective must be at most 200 characters")

    client = chromadb.PersistentClient(path=args.path)
    collection = client.get_or_create_collection(COLLECTION)
    if args.command != "query":
        save(collection, asyncio.run(retrieve(args)))
    elif collection.count() == 0:
        print("No saved research. Run search or fetch first.")
    else:
        results = collection.query(
            query_texts=[args.text], n_results=min(3, collection.count())
        )
        documents, metadatas = results["documents"], results["metadatas"]
        assert documents is not None and metadatas is not None
        for document, metadata in zip(documents[0], metadatas[0]):
            print(f"\n{metadata['title']}\n{metadata['url']}\n{document}")


if __name__ == "__main__":
    main()
