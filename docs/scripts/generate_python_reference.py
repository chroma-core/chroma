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


def get_documentation_sections() -> list[SectionConfig]:
    """Define all documentation sections. Import chromadb here to avoid import at module level."""
    import chromadb
    from chromadb.api import AdminAPI, ClientAPI
    from chromadb.api.models.Collection import Collection
    from chromadb.api.types import (
        Embedding,
        EmbeddingFunction,
        GetResult,
        QueryResult,
        Schema,
        SearchResult,
        SparseEmbeddingFunction,
    )
    from chromadb.base_types import SparseVector
    from chromadb.execution.expression.operator import Knn, Rrf, Select
    from chromadb.execution.expression.plan import Search

    return [
        SectionConfig(
            title="Clients",
            render_mode="function",
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
            title="Embedding Functions",
            render_mode="class",
            items=[
                ("EmbeddingFunction", EmbeddingFunction),
                ("SparseEmbeddingFunction", SparseEmbeddingFunction),
            ],
        ),
        SectionConfig(
            title="Types",
            render_mode="class",
            items=[
                ("Embedding", Embedding),
                ("SparseVector", SparseVector),
                ("Schema", Schema),
                ("Search", Search),
                ("Select", Select),
                ("Knn", Knn),
                ("Rrf", Rrf),
                ("GetResult", GetResult),
                ("QueryResult", QueryResult),
                ("SearchResult", SearchResult),
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

    description = parsed.short_description
    if parsed.long_description:
        description = (
            f"{description} {parsed.long_description}"
            if description
            else parsed.long_description
        )

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
        description=parsed.short_description,
        properties=properties,
        methods=methods,
    )


# =============================================================================
# MDX Rendering
# =============================================================================


def render_param(p: Param) -> str:
    """Render a parameter as a Mintlify ParamField component."""
    attrs = f'path="{p.name}" type="{p.type}"'
    if p.required:
        attrs += " required"

    if p.description:
        return f"<ParamField {attrs}>\n  {p.description.strip()}\n</ParamField>\n"
    return f"<ParamField {attrs} />\n"


def render_function(fn: FunctionDoc) -> str:
    """Render a function as MDX."""
    lines = [f"### {fn.name}\n"]
    if fn.description:
        lines.append(f"{fn.description}\n")
    lines.extend(render_param(p) for p in fn.params)
    return "\n".join(lines)


def render_method(method: MethodDoc, heading_level: int = 3) -> str:
    """Render a method as MDX, including returns and raises."""
    heading = "#" * heading_level
    lines = [f"{heading} {method.name}\n"]

    if method.description:
        lines.append(f"{method.description}\n")

    lines.extend(render_param(p) for p in method.params)

    if method.returns:
        lines.append(f"**Returns:** {method.returns}\n")

    if method.raises:
        lines.append("**Raises:**\n")
        lines.extend(f"- {exc}" for exc in method.raises)
        lines.append("")

    return "\n".join(lines)


def render_class(cls: ClassDoc, full_methods: bool = False) -> str:
    """Render a class as MDX."""
    lines = [f"### {cls.name}\n"]

    if cls.description:
        lines.append(f"{cls.description}\n")

    if cls.properties:
        lines.append('<span class="text-sm">Properties</span>\n')
        lines.extend(render_param(p) for p in cls.properties)

    if cls.methods:
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

    lines = [f"## {config.title}\n"]

    for item in config.items:
        if config.render_mode == "function":
            assert isinstance(item, tuple)
            name, fn = item
            lines.append(render_function(extract_function(fn, name)))
            lines.append("")

        elif config.render_mode == "method":
            assert isinstance(item, str)
            method_name = item
            method = getattr(config.source_class, method_name, None) or getattr(
                BaseAPI, method_name, None
            )
            if method:
                lines.append(render_method(extract_method(method, method_name)))
                lines.append("")

        elif config.render_mode in ("class", "class_full"):
            assert isinstance(item, tuple)
            name, cls = item

            if not inspect.isclass(cls):
                lines.append(f"### {name}\n")
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
                    class_doc, full_methods=(config.render_mode == "class_full")
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


def generate_documentation() -> str:
    """Generate the complete SDK reference documentation."""
    sections = get_documentation_sections()

    lines = [
        "---",
        'title: "Python Reference"',
        "---\n",
        INSTALLATION_SECTION,
        "---\n",
    ]

    for i, section in enumerate(sections):
        if i > 0:
            lines.append("---\n")
        lines.append(render_section(section))

    return "\n".join(lines)


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Python SDK reference documentation for Chroma",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --output reference/python/index.mdx
  %(prog)s  # prints to stdout
        """,
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output file path relative to docs/mintlify/ (default: stdout)",
    )
    args = parser.parse_args()

    content = generate_documentation()

    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            mintlify_dir = Path(__file__).parent.parent / "mintlify"
            output_path = mintlify_dir / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content)
        print(f"Generated: {output_path}")
    else:
        print(content)


if __name__ == "__main__":
    main()
