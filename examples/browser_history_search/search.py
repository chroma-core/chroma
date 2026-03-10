"""
Semantically search your browser history using natural language queries.

Run load_history.py first to populate the Chroma database.
"""

import argparse
from typing import Optional

import chromadb


def format_result(result: dict, rank: int) -> str:
    """Format a single search result for display."""
    title = result.get("title", "No title")
    url = result.get("url", "")
    domain = result.get("domain", "")
    visit_count = result.get("visit_count", 0)
    last_visit = result.get("last_visit_date", "Unknown")

    output = f"\n{rank}. {title}\n"
    output += f"   URL: {url}\n"
    output += f"   Domain: {domain} | Visits: {visit_count} | Last visited: {last_visit}"

    return output


def search(
    query: str,
    collection: chromadb.Collection,
    n_results: int = 10,
    domain_filter: Optional[str] = None,
) -> None:
    """
    Search the browser history collection.

    Args:
        query: Natural language search query
        collection: Chroma collection to search
        n_results: Number of results to return
        domain_filter: Optional domain to filter results (e.g., 'github.com')
    """
    # Build where filter if domain specified
    where_filter = None
    if domain_filter:
        where_filter = {"domain": {"$contains": domain_filter}}

    # Perform semantic search
    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where_filter,
        include=["documents", "metadatas", "distances"],
    )

    # Display results
    if not results["ids"][0]:
        print("\nNo results found.")
        if domain_filter:
            print(f"Try removing the domain filter (--domain {domain_filter})")
        return

    print(f"\n{'=' * 60}")
    print(f"Results for: '{query}'")
    if domain_filter:
        print(f"Filtered by domain: {domain_filter}")
    print(f"{'=' * 60}")

    for i, (doc_id, metadata, distance) in enumerate(
        zip(results["ids"][0], results["metadatas"][0], results["distances"][0]), 1
    ):
        print(format_result(metadata, i))

    print(f"\n{'=' * 60}")
    print(f"Found {len(results['ids'][0])} results")


def interactive_search(
    collection: chromadb.Collection,
    n_results: int = 10,
) -> None:
    """Run an interactive search session."""
    print("\nBrowser History Search")
    print("=" * 40)
    print("Enter your search query (Ctrl+C to quit)")
    print("Tip: Use natural language like 'that article about machine learning'\n")

    while True:
        try:
            query = input("Query: ").strip()

            if not query:
                print("Please enter a search query.\n")
                continue

            # Check for domain filter syntax: "query (filter by domain: example.com)"
            domain_filter = None
            if "(filter by domain:" in query.lower():
                import re

                match = re.search(r"\(filter by domain:\s*([^\)]+)\)", query, re.IGNORECASE)
                if match:
                    domain_filter = match.group(1).strip()
                    query = re.sub(r"\(filter by domain:[^\)]+\)", "", query).strip()

            search(query, collection, n_results, domain_filter)
            print()

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break


def main(
    collection_name: str = "browser_history",
    persist_directory: str = "chroma_storage",
    query: Optional[str] = None,
    n_results: int = 10,
    domain: Optional[str] = None,
) -> None:
    """
    Search browser history.

    Args:
        collection_name: Name of the Chroma collection
        persist_directory: Directory where Chroma database is stored
        query: Optional single query (runs interactive mode if not provided)
        n_results: Number of results to show
        domain: Optional domain filter
    """
    # Initialize Chroma client
    client = chromadb.PersistentClient(path=persist_directory)

    # Get the collection
    try:
        collection = client.get_collection(name=collection_name)
    except ValueError:
        print(f"Error: Collection '{collection_name}' not found.")
        print("Run 'python load_history.py' first to load your browser history.")
        return

    count = collection.count()
    print(f"Loaded collection with {count} history entries")

    if query:
        # Single query mode
        search(query, collection, n_results, domain)
    else:
        # Interactive mode
        interactive_search(collection, n_results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Semantically search your browser history"
    )

    parser.add_argument(
        "--collection_name",
        type=str,
        default="browser_history",
        help="Name of the Chroma collection (default: browser_history)",
    )
    parser.add_argument(
        "--persist_directory",
        type=str,
        default="chroma_storage",
        help="Directory where Chroma database is stored (default: chroma_storage)",
    )
    parser.add_argument(
        "--query",
        "-q",
        type=str,
        help="Search query (runs interactive mode if not provided)",
    )
    parser.add_argument(
        "--results",
        "-n",
        type=int,
        default=10,
        help="Number of results to show (default: 10)",
    )
    parser.add_argument(
        "--domain",
        "-d",
        type=str,
        help="Filter results by domain (e.g., 'github.com')",
    )

    args = parser.parse_args()

    main(
        collection_name=args.collection_name,
        persist_directory=args.persist_directory,
        query=args.query,
        n_results=args.results,
        domain=args.domain,
    )
