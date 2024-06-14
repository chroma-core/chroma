from collections import defaultdict, deque
from graphlib import TopologicalSorter
from typing import List, Dict

from chromadb.db.base import Cursor


def fetch_tables(cursor: Cursor) -> List[str]:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cursor.fetchall()]


def fetch_foreign_keys(cursor: Cursor, table_name: str) -> List[str]:
    cursor.execute(f"PRAGMA foreign_key_list({table_name});")
    return [row[2] for row in cursor.fetchall()]  # Table being referenced


def build_dependency_graph(tables: List[str], cursor: Cursor) -> Dict[str, List[str]]:
    graph = defaultdict(list)
    for table in tables:
        foreign_keys = fetch_foreign_keys(cursor, table)
        for fk_table in foreign_keys:
            graph[table].append(fk_table)
        if not foreign_keys and table not in graph.keys():
            graph[table] = []

    return graph


def topological_sort(graph: Dict[str, List[str]]) -> List[str]:
    ts = TopologicalSorter(graph)
    # Reverse the order since TopologicalSorter gives the order of dependencies,
    # but we want to drop tables in reverse dependency order
    return list(ts.static_order())[::-1]


def get_drop_order(cursor: Cursor) -> List[str]:
    tables = fetch_tables(cursor)
    filtered_tables = [
        table for table in tables if not table.startswith("embedding_fulltext_search_")
    ]
    graph = build_dependency_graph(filtered_tables, cursor)
    drop_order = topological_sort(graph)
    return drop_order
