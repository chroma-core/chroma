"""AST-aware code chunking with credential redaction.

This module extracts semantic chunks from source files (functions, classes,
markdown sections, top-level YAML keys) and produces deterministic hashes
for each chunk.  It falls back to sliding-window chunking for unsupported
file types.

Dependencies:  (none beyond stdlib; yaml support optional)
"""

from __future__ import annotations

import ast
import hashlib
import re
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path


class ChunkType(Enum):
    FUNCTION = auto()
    METHOD = auto()
    CLASS = auto()
    MODULE = auto()
    MARKDOWN_SECTION = auto()
    YAML_KEY = auto()
    JSON_KEY = auto()
    WINDOW = auto()


@dataclass(frozen=True, slots=True)
class Chunk:
    """A single semantic chunk extracted from a source file."""

    content: str
    chunk_type: ChunkType
    start_line: int
    end_line: int
    metadata: dict[str, str]
    file_path: str

    @property
    def id(self) -> str:
        """Deterministic ID for Chroma upsert — stable across re-runs."""
        payload = f"{self.file_path}:{self.start_line}:{self.end_line}:{self.content}"
        return hashlib.sha256(payload.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Credential redaction (applied per-line so AST chunking doesn't break)
# ---------------------------------------------------------------------------

_CREDENTIAL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"sk-[a-zA-Z0-9]{20,}"),
    re.compile(r"ghp_[a-zA-Z0-9]{36,}"),
    re.compile(r"glpat-[a-zA-Z0-9_-]{20,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"\b[A-Za-z0-9/+=]{40}\b"),  # broad AWS secret heuristic
    re.compile(r"private[_-]?key", re.IGNORECASE),
    re.compile(r"password\s*[=:]\s*[^\s#]{3,}", re.IGNORECASE),
    re.compile(r"secret\s*[=:]\s*[^\s#]{3,}", re.IGNORECASE),
    re.compile(r"token\s*[=:]\s*[^\s#]{3,}", re.IGNORECASE),
    re.compile(r"api[_-]?key\s*[=:]\s*[^\s#]{3,}", re.IGNORECASE),
    re.compile(r"DATABASE_URL\s*[=:]\s*[^:]+:[^@]+", re.IGNORECASE),
    re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    re.compile(r"-----BEGIN CERTIFICATE-----"),
]


def _redact_line(line: str) -> str | None:
    for pat in _CREDENTIAL_PATTERNS:
        if pat.search(line):
            return None
    return line


def _redact(content: str) -> str:
    lines = content.splitlines(keepends=True)
    redacted: list[str] = []
    changed = False
    for line in lines:
        if _redact_line(line) is None:
            redacted.append("# [CREDENTIAL REDACTED]\n")
            changed = True
        else:
            redacted.append(line)
    if not changed:
        return content
    return "".join(redacted).rstrip()


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def chunk_file(file_path: Path, max_tokens: int = 512) -> list[Chunk]:
    """Extract semantic chunks from *file_path*."""
    try:
        content = file_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    if not content.strip():
        return []

    ext = file_path.suffix.lower()
    results: list[Chunk]

    if ext == ".py":
        results = _chunk_python(content, str(file_path), max_tokens)
    elif ext == ".md":
        results = _chunk_markdown(content, str(file_path))
    elif ext in (".yaml", ".yml"):
        results = _chunk_yaml(content, str(file_path))
    elif ext == ".json":
        results = _chunk_json(content, str(file_path))
    else:
        results = _chunk_window(content, str(file_path), max_tokens)

    # Redact credentials in-place
    for r in results:
        object.__setattr__(r, "content", _redact(r.content))  # frozen dataclass workaround
    return results


# ---------------------------------------------------------------------------
# Python — AST-based
# ---------------------------------------------------------------------------

def _chunk_python(content: str, file_path: str, max_tokens: int) -> list[Chunk]:
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return _chunk_window(content, file_path, max_tokens)

    lines = content.splitlines(keepends=True)
    chunks: list[Chunk] = []
    covered: set[int] = set()

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start, end = node.lineno, node.end_lineno or node.lineno
            chunks.append(
                Chunk(
                    content="".join(lines[start - 1 : end]).rstrip(),
                    chunk_type=ChunkType.FUNCTION,
                    start_line=start,
                    end_line=end,
                    metadata={"name": node.name},
                    file_path=file_path,
                )
            )
            covered.update(range(start, end + 1))

        elif isinstance(node, ast.ClassDef):
            start, end = node.lineno, node.end_lineno or node.lineno
            methods = [
                n
                for n in ast.iter_child_nodes(node)
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            ]

            # If class is huge, split into method-level chunks + class header
            est_tokens = (end - start + 1) * 4
            if methods and est_tokens > max_tokens * 4:
                for m in methods:
                    ms, me = m.lineno, m.end_lineno or m.lineno
                    chunks.append(
                        Chunk(
                            content="".join(lines[ms - 1 : me]).rstrip(),
                            chunk_type=ChunkType.METHOD,
                            start_line=ms,
                            end_line=me,
                            metadata={"class": node.name, "name": m.name},
                            file_path=file_path,
                        )
                    )
                    covered.update(range(ms, me + 1))
                first_method = min(m.lineno for m in methods)
                if first_method > start + 1:
                    header = "".join(lines[start - 1 : first_method - 1])
                    if header.strip():
                        chunks.append(
                            Chunk(
                                content=header.rstrip(),
                                chunk_type=ChunkType.CLASS,
                                start_line=start,
                                end_line=first_method - 1,
                                metadata={"name": node.name},
                                file_path=file_path,
                            )
                        )
                covered.update(range(start, end + 1))
            else:
                chunks.append(
                    Chunk(
                        content="".join(lines[start - 1 : end]).rstrip(),
                        chunk_type=ChunkType.CLASS,
                        start_line=start,
                        end_line=end,
                        metadata={"name": node.name},
                        file_path=file_path,
                    )
                )
                covered.update(range(start, end + 1))

    # Module-level leftovers
    module_lines: list[str] = []
    mod_start: int | None = None
    for i, line in enumerate(lines, 1):
        if i not in covered and line.strip() and not line.strip().startswith("#"):
            if mod_start is None:
                mod_start = i
            module_lines.append(line)

    if module_lines and mod_start is not None:
        chunks.append(
            Chunk(
                content="".join(module_lines).rstrip(),
                chunk_type=ChunkType.MODULE,
                start_line=mod_start,
                end_line=mod_start + len(module_lines) - 1,
                metadata={},
                file_path=file_path,
            )
        )

    return chunks if chunks else _chunk_window(content, file_path, max_tokens)


# ---------------------------------------------------------------------------
# Markdown
# ---------------------------------------------------------------------------

def _chunk_markdown(content: str, file_path: str) -> list[Chunk]:
    header_pat = re.compile(r"^(#{1,3})\s+(.+)$", re.MULTILINE)
    lines = content.split("\n")
    matches = list(header_pat.finditer(content))

    if not matches:
        return _chunk_window(content, file_path, max_tokens=512)

    chunks: list[Chunk] = []
    positions = []
    for m in matches:
        line_num = content[: m.start()].count("\n") + 1
        positions.append((line_num, m.group(0)))

    for i, (line_num, header) in enumerate(positions):
        end_line = positions[i + 1][0] - 1 if i + 1 < len(positions) else len(lines)
        section = "\n".join(lines[line_num - 1 : end_line]).strip()
        if section:
            chunks.append(
                Chunk(
                    content=section,
                    chunk_type=ChunkType.MARKDOWN_SECTION,
                    start_line=line_num,
                    end_line=end_line,
                    metadata={"header": header.lstrip("#").strip()},
                    file_path=file_path,
                )
            )

    # Preamble before first header
    if positions and positions[0][0] > 1:
        preamble = "\n".join(lines[: positions[0][0] - 1]).strip()
        if preamble:
            chunks.insert(
                0,
                Chunk(
                    content=preamble,
                    chunk_type=ChunkType.MARKDOWN_SECTION,
                    start_line=1,
                    end_line=positions[0][0] - 1,
                    metadata={"header": "preamble"},
                    file_path=file_path,
                ),
            )
    return chunks


# ---------------------------------------------------------------------------
# YAML
# ---------------------------------------------------------------------------

def _chunk_yaml(content: str, file_path: str) -> list[Chunk]:
    try:
        import yaml
    except ImportError:
        return _chunk_window(content, file_path, max_tokens=512)

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError:
        return _chunk_window(content, file_path, max_tokens=512)

    if not isinstance(data, dict):
        return _chunk_window(content, file_path, max_tokens=512)

    lines = content.split("\n")
    key_positions: list[tuple[int, str]] = []
    for i, line in enumerate(lines, 1):
        match = re.match(r"^(\S+)\s*:", line)
        if match:
            key_positions.append((i, match.group(1)))

    chunks: list[Chunk] = []
    for i, (line_num, key) in enumerate(key_positions):
        end_line = key_positions[i + 1][0] - 1 if i + 1 < len(key_positions) else len(lines)
        section = "\n".join(lines[line_num - 1 : end_line]).strip()
        if section:
            chunks.append(
                Chunk(
                    content=section,
                    chunk_type=ChunkType.YAML_KEY,
                    start_line=line_num,
                    end_line=end_line,
                    metadata={"key": key},
                    file_path=file_path,
                )
            )
    return chunks if chunks else _chunk_window(content, file_path, max_tokens=512)


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

def _chunk_json(content: str, file_path: str) -> list[Chunk]:
    import json as json_mod

    try:
        data = json_mod.loads(content)
    except (json_mod.JSONDecodeError, ValueError):
        return _chunk_window(content, file_path, max_tokens=512)

    if not isinstance(data, dict):
        return _chunk_window(content, file_path, max_tokens=512)

    chunks: list[Chunk] = []
    for key, value in data.items():
        serialized = json_mod.dumps({key: value}, indent=2)
        chunks.append(
            Chunk(
                content=serialized,
                chunk_type=ChunkType.JSON_KEY,
                start_line=1,
                end_line=1,
                metadata={"key": key},
                file_path=file_path,
            )
        )
    return chunks if chunks else _chunk_window(content, file_path, max_tokens=512)


# ---------------------------------------------------------------------------
# Sliding-window fallback
# ---------------------------------------------------------------------------

def _chunk_window(content: str, file_path: str, max_tokens: int) -> list[Chunk]:
    chars_per_chunk = max_tokens * 4
    overlap_chars = max_tokens * 4 // 8  # 12.5% overlap
    stride = max(1, chars_per_chunk - overlap_chars)

    chunks: list[Chunk] = []
    current = 0
    total = len(content)

    while current < total:
        end = min(current + chars_per_chunk, total)
        text = content[current:end]

        start_line = content[:current].count("\n") + 1
        end_line = content[:end].count("\n") + 1

        if text.strip():
            chunks.append(
                Chunk(
                    content=text.strip(),
                    chunk_type=ChunkType.WINDOW,
                    start_line=start_line,
                    end_line=end_line,
                    metadata={},
                    file_path=file_path,
                )
            )
        if end >= total:
            break
        current += stride

    return chunks
