"""
Load browser history into Chroma for semantic search.

Supports Chrome and Firefox browsers. Extracts page titles, URLs, and visit
metadata, then creates embeddings for semantic search.
"""

import argparse
import glob
import os
import platform
import shutil
import sqlite3
import tempfile
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from tqdm import tqdm

import chromadb


def get_chrome_history_path() -> Optional[str]:
    """Get the default Chrome history database path for the current platform."""
    system = platform.system()

    if system == "Darwin":  # macOS
        path = os.path.expanduser(
            "~/Library/Application Support/Google/Chrome/Default/History"
        )
    elif system == "Linux":
        path = os.path.expanduser("~/.config/google-chrome/Default/History")
    elif system == "Windows":
        path = os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            "Google",
            "Chrome",
            "User Data",
            "Default",
            "History",
        )
    else:
        return None

    return path if os.path.exists(path) else None


def get_firefox_history_path() -> Optional[str]:
    """Get the default Firefox history database path for the current platform."""
    system = platform.system()

    if system == "Darwin":  # macOS
        profile_dir = os.path.expanduser("~/Library/Application Support/Firefox/Profiles")
    elif system == "Linux":
        profile_dir = os.path.expanduser("~/.mozilla/firefox")
    elif system == "Windows":
        profile_dir = os.path.join(
            os.environ.get("APPDATA", ""), "Mozilla", "Firefox", "Profiles"
        )
    else:
        return None

    # Find the default profile (usually ends with .default or .default-release)
    if os.path.exists(profile_dir):
        profiles = glob.glob(os.path.join(profile_dir, "*.default*", "places.sqlite"))
        if profiles:
            return profiles[0]

    return None


def copy_history_db(source_path: str) -> str:
    """
    Copy the history database to a temporary file.

    Browsers lock their databases, so we need to work with a copy.
    """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
    shutil.copy2(source_path, temp_file.name)
    return temp_file.name


def extract_chrome_history(db_path: str, limit: Optional[int] = None) -> list[dict]:
    """
    Extract history entries from a Chrome history database.

    Returns a list of dictionaries with url, title, visit_count, and last_visit_time.
    """
    temp_db = copy_history_db(db_path)
    entries = []

    try:
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        # Chrome stores timestamps as microseconds since Jan 1, 1601
        # We convert to Unix timestamp
        query = """
            SELECT
                url,
                title,
                visit_count,
                (last_visit_time / 1000000) - 11644473600 as last_visit_unix
            FROM urls
            WHERE title IS NOT NULL AND title != ''
            ORDER BY last_visit_time DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query)

        for row in cursor.fetchall():
            url, title, visit_count, last_visit_unix = row
            try:
                last_visit = datetime.fromtimestamp(last_visit_unix)
            except (ValueError, OSError):
                last_visit = None

            entries.append(
                {
                    "url": url,
                    "title": title,
                    "visit_count": visit_count,
                    "last_visit": last_visit,
                    "domain": urlparse(url).netloc,
                }
            )

        conn.close()
    finally:
        os.unlink(temp_db)

    return entries


def extract_firefox_history(db_path: str, limit: Optional[int] = None) -> list[dict]:
    """
    Extract history entries from a Firefox places.sqlite database.

    Returns a list of dictionaries with url, title, visit_count, and last_visit_time.
    """
    temp_db = copy_history_db(db_path)
    entries = []

    try:
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        # Firefox stores timestamps as microseconds since Unix epoch
        query = """
            SELECT
                p.url,
                p.title,
                p.visit_count,
                p.last_visit_date / 1000000 as last_visit_unix
            FROM moz_places p
            WHERE p.title IS NOT NULL AND p.title != ''
            ORDER BY p.last_visit_date DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query)

        for row in cursor.fetchall():
            url, title, visit_count, last_visit_unix = row
            try:
                last_visit = datetime.fromtimestamp(last_visit_unix) if last_visit_unix else None
            except (ValueError, OSError):
                last_visit = None

            entries.append(
                {
                    "url": url,
                    "title": title,
                    "visit_count": visit_count or 0,
                    "last_visit": last_visit,
                    "domain": urlparse(url).netloc,
                }
            )

        conn.close()
    finally:
        os.unlink(temp_db)

    return entries


def main(
    browser: Optional[str] = None,
    history_path: Optional[str] = None,
    collection_name: str = "browser_history",
    persist_directory: str = "chroma_storage",
    limit: Optional[int] = None,
) -> None:
    """
    Load browser history into Chroma.

    Args:
        browser: Browser to use ('chrome' or 'firefox'). Auto-detected if not specified.
        history_path: Custom path to history database. Uses default if not specified.
        collection_name: Name of the Chroma collection to create.
        persist_directory: Directory to store the Chroma database.
        limit: Maximum number of entries to load (None for all).
    """
    # Determine history path and extraction function
    if history_path:
        # User specified a path - try to auto-detect format
        if not os.path.exists(history_path):
            print(f"Error: History file not found at {history_path}")
            return

        # Check filename to guess browser
        if "places.sqlite" in history_path.lower():
            extract_func = extract_firefox_history
            browser_name = "Firefox"
        else:
            extract_func = extract_chrome_history
            browser_name = "Chrome"
    elif browser:
        browser = browser.lower()
        if browser == "chrome":
            history_path = get_chrome_history_path()
            extract_func = extract_chrome_history
            browser_name = "Chrome"
        elif browser == "firefox":
            history_path = get_firefox_history_path()
            extract_func = extract_firefox_history
            browser_name = "Firefox"
        else:
            print(f"Error: Unknown browser '{browser}'. Use 'chrome' or 'firefox'.")
            return

        if not history_path:
            print(f"Error: Could not find {browser_name} history database.")
            print("Please specify the path manually with --history_path")
            return
    else:
        # Auto-detect: try Chrome first, then Firefox
        history_path = get_chrome_history_path()
        if history_path:
            extract_func = extract_chrome_history
            browser_name = "Chrome"
        else:
            history_path = get_firefox_history_path()
            if history_path:
                extract_func = extract_firefox_history
                browser_name = "Firefox"
            else:
                print("Error: Could not find browser history database.")
                print("Please specify the browser with --browser or path with --history_path")
                return

    print(f"Loading {browser_name} history from: {history_path}")

    # Extract history entries
    print("Extracting history entries...")
    entries = extract_func(history_path, limit)

    if not entries:
        print("No history entries found.")
        return

    print(f"Found {len(entries)} history entries")

    # Prepare data for Chroma
    documents = []
    metadatas = []
    ids = []

    for i, entry in enumerate(entries):
        # Combine title and URL for better semantic matching
        doc_text = f"{entry['title']} - {entry['url']}"
        documents.append(doc_text)

        metadata = {
            "url": entry["url"],
            "title": entry["title"],
            "domain": entry["domain"],
            "visit_count": entry["visit_count"],
        }

        if entry["last_visit"]:
            metadata["last_visit"] = entry["last_visit"].isoformat()
            metadata["last_visit_date"] = entry["last_visit"].strftime("%Y-%m-%d")

        metadatas.append(metadata)
        ids.append(f"history_{i}")

    # Initialize Chroma
    print(f"Initializing Chroma at: {persist_directory}")
    client = chromadb.PersistentClient(path=persist_directory)

    # Create or get collection
    collection = client.get_or_create_collection(
        name=collection_name,
        metadata={"description": "Browser history for semantic search"},
    )

    # Check existing count
    existing_count = collection.count()
    if existing_count > 0:
        print(f"Collection already contains {existing_count} entries")
        overwrite = input("Overwrite existing data? (y/n): ").lower().strip()
        if overwrite == "y":
            client.delete_collection(name=collection_name)
            collection = client.create_collection(
                name=collection_name,
                metadata={"description": "Browser history for semantic search"},
            )
        else:
            print("Aborting. Use a different collection name to keep existing data.")
            return

    # Load documents in batches
    batch_size = 100
    for i in tqdm(range(0, len(documents), batch_size), desc="Adding to Chroma"):
        batch_end = min(i + batch_size, len(documents))
        collection.add(
            ids=ids[i:batch_end],
            documents=documents[i:batch_end],
            metadatas=metadatas[i:batch_end],
        )

    print(f"\nSuccessfully loaded {len(documents)} history entries into Chroma!")
    print(f"Run 'python search.py' to search your history.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load browser history into Chroma for semantic search"
    )

    parser.add_argument(
        "--browser",
        type=str,
        choices=["chrome", "firefox"],
        help="Browser to load history from (auto-detected if not specified)",
    )
    parser.add_argument(
        "--history_path",
        type=str,
        help="Custom path to the browser history database",
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
        help="Directory to store the Chroma database (default: chroma_storage)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of history entries to load",
    )

    args = parser.parse_args()

    main(
        browser=args.browser,
        history_path=args.history_path,
        collection_name=args.collection_name,
        persist_directory=args.persist_directory,
        limit=args.limit,
    )
