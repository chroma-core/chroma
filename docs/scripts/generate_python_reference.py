#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "docstring-parser>=0.15",
#     "chromadb",
# ]
# ///
"""
Generate Python SDK reference documentation for Chroma.

Usage:
    uv run generate_python_reference.py --output reference/python/index.mdx

This script introspects the chromadb package and generates MDX documentation
with ParamField components for Mintlify.

To extend this script:
1. Add new sections to get_documentation_sections()
2. Add type simplifications to TYPE_SIMPLIFICATIONS
"""

from __future__ import annotations

import argparse
import inspect
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Optional,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from docstring_parser import parse as parse_docstring


# =============================================================================
# Configuration
# =============================================================================

TYPE_SIMPLIFICATIONS: dict[str, str] = {
    "ndarray": "Embedding",
    "Sequence[float]": "Embedding",
    "Sequence[int]": "Embedding",
    "List[Union[Sequence[float], Sequence[int]]]": "Embeddings",
    "Mapping[str, Union[str, int, float, bool, SparseVector, None]]": "Metadata",
    "List[Mapping[str, Union[str, int, float, bool, SparseVector, None]]]": "Metadatas",
}

TYPE_ALIASES: dict[str, str] = {
    "Union[str, List[str]]": "OneOrMany[str]",
    "Union[str, List[str], None]": "Optional[OneOrMany[str]]",
    "List[str]": "IDs",
    "Optional[List[str]]": "Optional[IDs]",
}


@dataclass
class SectionConfig:
    """Configuration for a documentation section."""

    title: str
    items: list[tuple[str, Any]] | list[str]
    source_class: Optional[type] = None
    render_mode: str = (
        "function"  # "function", "method", "class", "class_full", "type_alias"
    )
    output_file: str = "index"
    show_class_methods: bool = True


def get_documentation_sections() -> list[SectionConfig]:
    """Define all documentation sections with output_file for split generation."""
    import chromadb
    from chromadb.api import AdminAPI, ClientAPI
    from chromadb.api.models.Collection import Collection
    from chromadb.api.types import (
        BoolInvertedIndexConfig,
        Embedding,
        EmbeddingFunction,
        FloatInvertedIndexConfig,
        FtsIndexConfig,
        GetResult,
        HnswIndexConfig,
        IntInvertedIndexConfig,
        QueryResult,
        Schema,
        SearchResult,
        SparseEmbeddingFunction,
        SparseVectorIndexConfig,
        SpannIndexConfig,
        StringInvertedIndexConfig,
        VectorIndexConfig,
    )
    from chromadb.base_types import SparseVector
    from chromadb.execution.expression.operator import (
        GroupBy,
        Knn,
        Limit,
        MaxK,
        MinK,
        Rrf,
        Select,
    )
    from chromadb.execution.expression.plan import Search
    from chromadb.utils import embedding_functions as ef_module

    return [
        SectionConfig(
            title="Clients",
            render_mode="function",
            output_file="client",
            items=[
                ("EphemeralClient", chromadb.EphemeralClient),
                ("PersistentClient", chromadb.PersistentClient),
                ("HttpClient", chromadb.HttpClient),
                ("AsyncHttpClient", chromadb.AsyncHttpClient),
                ("CloudClient", chromadb.CloudClient),
                ("AdminClient", chromadb.AdminClient),
            ],
        ),
        SectionConfig(
            title="Client Methods",
            render_mode="method",
            source_class=ClientAPI,
            output_file="client",
            items=[
                "heartbeat",
                "list_collections",
                "count_collections",
                "create_collection",
                "get_collection",
                "get_or_create_collection",
                "delete_collection",
                "reset",
                "get_version",
                "get_settings",
                "get_max_batch_size",
            ],
        ),
        SectionConfig(
            title="Admin Client Methods",
            render_mode="method",
            source_class=AdminAPI,
            output_file="client",
            items=[
                "create_tenant",
                "get_tenant",
                "create_database",
                "get_database",
                "delete_database",
                "list_databases",
            ],
        ),
        SectionConfig(
            title="Collection Methods",
            render_mode="method",
            source_class=Collection,
            output_file="collection",
            items=[
                "count",
                "add",
                "get",
                "peek",
                "query",
                "modify",
                "update",
                "upsert",
                "delete",
            ],
        ),
        SectionConfig(
            title="Types",
            render_mode="class",
            output_file="collection",
            items=[
                ("GetResult", GetResult),
                ("QueryResult", QueryResult),
            ],
        ),
        SectionConfig(
            title="Embedding Function Base Classes",
            render_mode="class",
            output_file="embedding-functions",
            items=[
                ("EmbeddingFunction", EmbeddingFunction),
                ("SparseEmbeddingFunction", SparseEmbeddingFunction),
            ],
        ),
        SectionConfig(
            title="Registration",
            render_mode="function",
            output_file="embedding-functions",
            items=[
                ("register_embedding_function", ef_module.register_embedding_function),
                (
                    "register_sparse_embedding_function",
                    ef_module.register_sparse_embedding_function,
                ),
            ],
        ),
        SectionConfig(
            title="Types",
            render_mode="class",
            output_file="embedding-functions",
            items=[
                ("Embedding", Embedding),
                ("SparseVector", SparseVector),
            ],
        ),
        SectionConfig(
            title="Search",
            render_mode="class",
            output_file="search",
            items=[("Search", Search)],
        ),
        SectionConfig(
            title="Select",
            render_mode="class",
            output_file="search",
            items=[("Select", Select)],
        ),
        SectionConfig(
            title="Knn",
            render_mode="class",
            output_file="search",
            items=[("Knn", Knn)],
        ),
        SectionConfig(
            title="Rrf",
            render_mode="class",
            output_file="search",
            items=[("Rrf", Rrf)],
        ),
        SectionConfig(
            title="Group By",
            render_mode="class",
            output_file="search",
            items=[
                ("GroupBy", GroupBy),
                ("Limit", Limit),
                ("MinK", MinK),
                ("MaxK", MaxK),
            ],
        ),
        SectionConfig(
            title="SearchResult",
            render_mode="class",
            output_file="search",
            items=[("SearchResult", SearchResult)],
        ),
        SectionConfig(
            title="Schema",
            render_mode="class",
            output_file="schema",
            show_class_methods=False,
            items=[("Schema", Schema)],
        ),
        SectionConfig(
            title="Index configs",
            render_mode="class",
            output_file="schema",
            show_class_methods=False,
            items=[
                ("FtsIndexConfig", FtsIndexConfig),
                ("HnswIndexConfig", HnswIndexConfig),
                ("SpannIndexConfig", SpannIndexConfig),
                ("VectorIndexConfig", VectorIndexConfig),
                ("SparseVectorIndexConfig", SparseVectorIndexConfig),
                ("StringInvertedIndexConfig", StringInvertedIndexConfig),
                ("IntInvertedIndexConfig", IntInvertedIndexConfig),
                ("FloatInvertedIndexConfig", FloatInvertedIndexConfig),
                ("BoolInvertedIndexConfig", BoolInvertedIndexConfig),
            ],
        ),
    ]


# =============================================================================
# Data Model
# =============================================================================


@dataclass
class Param:
    """A parameter or property with type and optional description."""

    name: str
    type: str
    description: Optional[str] = None
    required: bool = False


@dataclass
class MethodDoc:
    """Documentation for a method."""

    name: str
    description: Optional[str] = None
    params: list[Param] = field(default_factory=list)
    returns: Optional[str] = None
    raises: list[str] = field(default_factory=list)
    is_async: bool = False


@dataclass
class FunctionDoc:
    """Documentation for a function."""

    name: str
    description: Optional[str] = None
    params: list[Param] = field(default_factory=list)
    returns: Optional[str] = None
    is_async: bool = False


@dataclass
class ClassDoc:
    """Documentation for a class."""

    name: str
    description: Optional[str] = None
    properties: list[Param] = field(default_factory=list)
    methods: list[MethodDoc] = field(default_factory=list)


# =============================================================================
# Type Formatting
# =============================================================================


def simplify_type(type_str: str) -> str:
    """Simplify complex type strings to more readable forms."""
    for pattern, replacement in TYPE_SIMPLIFICATIONS.items():
        if pattern in type_str:
            type_str = type_str.replace(pattern, replacement)

    for pattern, replacement in TYPE_ALIASES.items():
        if type_str == pattern:
            return replacement

    if "ForwardRef" in type_str:
        type_str = re.sub(r"ForwardRef\('(\w+)'\)", r"\1", type_str)

    if "Literal[" in type_str and type_str.count("Literal[") > 2:
        return "Where"

    if len(type_str) > 80:
        if "ndarray" in type_str.lower() or "embedding" in type_str.lower():
            return (
                "Optional[Embeddings]"
                if "List[" in type_str or "Union[" in type_str
                else "Optional[Embedding]"
            )
        if "Mapping" in type_str or "metadata" in type_str.lower():
            return (
                "Optional[Metadatas]" if "List[" in type_str else "Optional[Metadata]"
            )
        if "DataLoader" in type_str:
            return "Optional[DataLoader]"
        if "EmbeddingFunction" in type_str:
            return "Optional[EmbeddingFunction]"

    return type_str


def format_type(typ: Any) -> str:
    """Format a type annotation as a readable string."""
    if typ is None or typ is type(None):
        return "None"

    if isinstance(typ, str):
        return simplify_type(typ)

    origin = get_origin(typ)
    args = get_args(typ)

    if origin is Union:
        if len(args) == 2 and type(None) in args:
            inner = args[0] if args[1] is type(None) else args[1]
            return simplify_type(f"Optional[{format_type(inner)}]")
        return simplify_type(f"Union[{', '.join(format_type(a) for a in args)}]")

    if origin is not None:
        origin_name = getattr(origin, "__name__", str(origin))
        name_map = {"list": "List", "dict": "Dict", "tuple": "Tuple", "set": "Set"}
        origin_name = name_map.get(origin_name, origin_name)

        if args:
            return simplify_type(
                f"{origin_name}[{', '.join(format_type(a) for a in args)}]"
            )
        return origin_name

    if hasattr(typ, "__name__"):
        return cast(str, typ.__name__)

    return simplify_type(str(typ).replace("typing.", ""))


# =============================================================================
# Extraction
# =============================================================================


def _full_description(parsed: Any) -> Optional[str]:
    """Build full description from parsed docstring (short + long, paragraphs preserved)."""
    parts = []
    if getattr(parsed, "short_description", None):
        parts.append(parsed.short_description)
    if getattr(parsed, "long_description", None):
        parts.append(parsed.long_description)
    if not parts:
        return None
    return "\n\n".join(parts).strip() or None


def extract_function(fn: Callable[..., Any], name: Optional[str] = None) -> FunctionDoc:
    """Extract documentation from a function."""
    fn_name = name or fn.__name__

    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        sig = None

    try:
        type_hints = get_type_hints(fn)
    except Exception:
        type_hints = {}

    doc = inspect.getdoc(fn) or ""
    parsed = parse_docstring(doc)

    description = _full_description(parsed)

    param_descs = {p.arg_name: p.description for p in parsed.params}

    params = []
    if sig:
        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls"):
                continue

            param_type = type_hints.get(param_name, param.annotation)
            if param_type is inspect.Parameter.empty:
                param_type = "Any"

            params.append(
                Param(
                    name=param_name,
                    type=format_type(param_type),
                    description=param_descs.get(param_name),
                    required=param.default is inspect.Parameter.empty,
                )
            )

    return FunctionDoc(
        name=fn_name,
        description=description,
        params=params,
        returns=parsed.returns.description if parsed.returns else None,
        is_async=inspect.iscoroutinefunction(fn),
    )


def extract_method(fn: Callable[..., Any], name: Optional[str] = None) -> MethodDoc:
    """Extract documentation from a method, including raises information."""
    fn_doc = extract_function(fn, name)

    doc = inspect.getdoc(fn) or ""
    parsed = parse_docstring(doc)

    raises = []
    for exc in parsed.raises:
        if exc.type_name:
            raises.append(
                f"{exc.type_name}: {exc.description}"
                if exc.description
                else exc.type_name
            )

    return MethodDoc(
        name=fn_doc.name,
        description=fn_doc.description,
        params=fn_doc.params,
        returns=fn_doc.returns,
        raises=raises,
        is_async=fn_doc.is_async,
    )


def extract_class(cls: type) -> ClassDoc:
    """Extract documentation from a class, including properties and methods."""
    doc = cls.__doc__
    if doc and doc.startswith("dict("):
        doc = None
    doc = doc or ""
    parsed = parse_docstring(doc)

    properties = []
    for name, typ in getattr(cls, "__annotations__", {}).items():
        if not name.startswith("_"):
            properties.append(Param(name=name, type=format_type(typ)))

    methods = []
    for name, member in inspect.getmembers(cls):
        if name.startswith("_") and name != "__init__":
            continue
        if not (inspect.isfunction(member) or inspect.ismethod(member)):
            continue
        try:
            methods.append(extract_method(member, name))
        except Exception:
            pass

    methods.sort(key=lambda m: m.name)

    return ClassDoc(
        name=cls.__name__,
        description=_full_description(parsed),
        properties=properties,
        methods=methods,
    )


# =============================================================================
# MDX Rendering
# =============================================================================


def _mdx_text(text: str) -> str:
    """Escape { and } so MDX does not parse them as JS; use backslash so markdown still renders (paragraphs, code blocks)."""
    return text.replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")


def render_param(p: Param) -> str:
    """Render a parameter as a Mintlify ParamField component."""
    attrs = f'path="{p.name}" type="{p.type}"'
    if p.required:
        attrs += " required"

    if p.description:
        return f"<ParamField {attrs}>\n  {_mdx_text(p.description.strip())}\n</ParamField>\n"
    return f"<ParamField {attrs} />\n"


def render_function(fn: FunctionDoc, heading_level: int = 3) -> str:
    """Render a function as MDX."""
    lines = [f"{'#' * heading_level} {fn.name}\n"]
    if fn.description:
        lines.append(f"{_mdx_text(fn.description)}\n")
    lines.extend(render_param(p) for p in fn.params)
    return "\n".join(lines)


def render_method(method: MethodDoc, heading_level: int = 3) -> str:
    """Render a method as MDX, including returns and raises."""
    heading = "#" * heading_level
    lines = [f"{heading} {method.name}\n"]

    if method.description:
        lines.append(f"{_mdx_text(method.description)}\n")

    lines.extend(render_param(p) for p in method.params)

    if method.returns:
        lines.append(f"**Returns:** {_mdx_text(method.returns)}\n")

    if method.raises:
        lines.append("**Raises:**\n")
        lines.extend(f"- {_mdx_text(exc)}" for exc in method.raises)
        lines.append("")

    return "\n".join(lines)


def render_class(
    cls: ClassDoc,
    full_methods: bool = False,
    heading_level: int = 3,
    show_methods: bool = True,
) -> str:
    """Render a class as MDX."""
    lines = [f"{'#' * heading_level} {cls.name}\n"]

    if cls.description:
        lines.append(f"{_mdx_text(cls.description)}\n")

    if cls.properties:
        lines.append('<span class="text-sm">Properties</span>\n')
        lines.extend(render_param(p) for p in cls.properties)

    if show_methods and cls.methods:
        if full_methods:
            lines.append('<span class="text-sm">Methods</span>\n')
            lines.extend(render_method(m, heading_level=4) for m in cls.methods)
        else:
            lines.append('\n<span class="text-sm">Methods</span>\n')
            lines.append(", ".join(f"`{m.name}()`" for m in cls.methods) + "\n")

    return "\n".join(lines)


# =============================================================================
# Document Generation
# =============================================================================


def render_section(config: SectionConfig) -> str:
    """Render a complete documentation section based on its configuration."""
    from chromadb.api import BaseAPI

    single_item_name: Optional[str] = None
    if len(config.items) == 1:
        item = config.items[0]
        if config.render_mode == "function" and isinstance(item, tuple):
            single_item_name = item[0]
        elif config.render_mode == "method" and isinstance(item, str):
            single_item_name = item
        elif config.render_mode in ("class", "class_full") and isinstance(item, tuple):
            single_item_name = item[0]

    skip_section_heading = (
        single_item_name is not None and single_item_name == config.title
    )
    heading = 2 if skip_section_heading else 3

    lines = [] if skip_section_heading else [f"## {config.title}\n"]

    for item in config.items:
        if config.render_mode == "function":
            assert isinstance(item, tuple)
            name, fn = item
            lines.append(
                render_function(extract_function(fn, name), heading_level=heading)
            )
            lines.append("")

        elif config.render_mode == "method":
            assert isinstance(item, str)
            method_name = item
            method = getattr(config.source_class, method_name, None) or getattr(
                BaseAPI, method_name, None
            )
            if method:
                lines.append(
                    render_method(
                        extract_method(method, method_name), heading_level=heading
                    )
                )
                lines.append("")

        elif config.render_mode in ("class", "class_full"):
            assert isinstance(item, tuple)
            name, cls = item

            if not inspect.isclass(cls):
                lines.append(f"{'#' * heading} {name}\n")
                lines.append(f"`{format_type(cls)}`\n")
                lines.append("")
                continue

            class_doc = extract_class(cls)

            if not class_doc.description:
                doc = inspect.getdoc(cls)
                if doc and not doc.startswith("dict("):
                    class_doc.description = doc

            lines.append(
                render_class(
                    class_doc,
                    full_methods=(config.render_mode == "class_full"),
                    heading_level=heading,
                    show_methods=getattr(config, "show_class_methods", True),
                )
            )
            lines.append("")

    return "\n".join(lines)


INSTALLATION_SECTION = """## Installation

<CodeGroup>
```bash pip
pip install chromadb
```
```bash poetry
poetry add chromadb
```
```bash uv
uv pip install chromadb
```
</CodeGroup>
"""


FILE_TITLES: dict[str, str] = {
    "client": "Client",
    "collection": "Collection",
    "embedding-functions": "Embedding Functions",
    "search": "Search",
    "schema": "Schema",
}


def get_sections_by_file() -> dict[str, list[SectionConfig]]:
    """Group section configs by output file."""
    by_file: dict[str, list[SectionConfig]] = {}
    for config in get_documentation_sections():
        by_file.setdefault(config.output_file, []).append(config)
    return by_file


def generate_documentation_per_file() -> dict[str, str]:
    """Generate documentation as a dict of filename -> content. No index.mdx; /reference/python redirects to client."""
    by_file = get_sections_by_file()
    out: dict[str, str] = {}

    for file_stem, configs in by_file.items():
        if file_stem == "index":
            continue
        title = FILE_TITLES.get(file_stem, file_stem.replace("-", " ").title())
        lines = [
            "---",
            f'title: "{title}"',
            "---\n",
        ]
        for i, config in enumerate(configs):
            if i > 0:
                lines.append("---\n")
            lines.append(render_section(config))
        out[f"{file_stem}.mdx"] = "\n".join(lines)

    return out


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Python SDK reference documentation for Chroma",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  %(prog)s --output reference/python/
        """,
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        required=True,
        help="Output directory (e.g. reference/python/)",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    if not output_path.is_absolute():
        mintlify_dir = Path(__file__).parent.parent / "mintlify"
        output_path = mintlify_dir / output_path
    out_dir = output_path.resolve()
    if out_dir.suffix:
        out_dir = out_dir.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in generate_documentation_per_file().items():
        fpath = out_dir / filename
        fpath.write_text(content)
        print(f"Generated: {fpath}")


if __name__ == "__main__":
    main()
