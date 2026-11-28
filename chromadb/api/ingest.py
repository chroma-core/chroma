from __future__ import annotations

import datetime as _dt
import hashlib
import json
import math
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

_SCALARS = (str, bool, int, float)
DEFAULT_EMPTY_FILL = {"_ingested": True}


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float):
        return not math.isfinite(v)  # NaN, +inf, -inf
    # pd.NA / pd.NaT detected by name so pandas isn't imported
    return type(v).__name__ in ("NAType", "NaTType")


def sanitize_value(v: Any, *, nested: str = "json") -> Any:
    """Return a Chroma-safe scalar (str/int/float/bool) or None for 'missing'."""
    if v is None:
        return None

    if type(v).__module__ == "numpy":
        if type(v).__name__.startswith("datetime64"):
            return None if str(v) == "NaT" else str(v)
        if hasattr(v, "tolist"):          # numpy scalar or array -> native Python
            v = v.tolist()

    if _is_missing(v):
        return None
    if isinstance(v, _SCALARS):
        return v
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):   # includes pd.Timestamp
        return v.isoformat()
    if isinstance(v, (list, tuple, set, frozenset, dict)):
        if nested == "json":
            if isinstance(v, (set, frozenset)):
                v = sorted(v, key=str)
            return json.dumps(v, default=str)
        if nested == "drop":
            return None
        raise ValueError(f"Nested value not allowed in metadata: {v!r}")
    return str(v)   # Decimal, UUID, Enum, Path, ...


def sanitize_metadata(
    row: Mapping[str, Any], *, nested: str = "json", drop_none: bool = True
) -> dict:
    out = {}
    for k, v in row.items():
        sv = sanitize_value(v, nested=nested)
        if sv is None and drop_none:
            continue
        out[str(k)] = sv
    return out


@dataclass
class Prepared:
    ids: List[str]
    documents: Optional[List[str]] = None
    metadatas: Optional[List[dict]] = None
    embeddings: Optional[List[List[float]]] = None

    def __len__(self) -> int:
        return len(self.ids)

    def batches(self, size: int):
        for s in range(0, len(self), size):
            e = s + size
            kw: Dict[str, Any] = {"ids": self.ids[s:e]}
            if self.documents is not None:
                kw["documents"] = self.documents[s:e]
            if self.metadatas is not None:
                kw["metadatas"] = self.metadatas[s:e]
            if self.embeddings is not None:
                kw["embeddings"] = self.embeddings[s:e]
            yield kw


def _listlike(v: Any) -> bool:
    return isinstance(v, (list, tuple)) or getattr(v, "ndim", 0) >= 1


def is_tabular(obj: Any) -> bool:
    """True for inputs this library handles. Used by the .add() overload."""
    if obj is None:
        return False
    if type(obj).__name__ == "DataFrame" and hasattr(obj, "to_dict"):
        return True
    if isinstance(obj, Mapping):
        return True
    return (
        isinstance(obj, (list, tuple))
        and len(obj) > 0
        and all(isinstance(r, Mapping) for r in obj)
    )


def _to_columns(data: Any) -> Dict[str, list]:
    # DataFrame (duck-typed, pandas/polars stays optional)
    if type(data).__name__ == "DataFrame" and hasattr(data, "to_dict"):
        try:
            return {str(k): list(v) for k, v in data.to_dict(orient="list").items()}
        except TypeError:
            try:
                return {str(k): list(v) for k, v in data.to_dict(as_series=False).items()}
            except Exception:
                return {str(k): list(v) for k, v in data.to_dict().items()}

    # dict
    if isinstance(data, Mapping):
        if data and not all(_listlike(v) for v in data.values()):
            return {str(k): [v] for k, v in data.items()}          # single record
        cols = {str(k): list(v) for k, v in data.items()}
        lengths = {len(v) for v in cols.values()}
        if len(lengths) > 1:
            raise ValueError(
                f"Dict columns have different lengths: { {k: len(v) for k, v in cols.items()} }. "
                "For a single record containing list values, pass it as [record]."
            )
        return cols

    # list of records
    if isinstance(data, (list, tuple)) and all(isinstance(r, Mapping) for r in data):
        keys: List[str] = []
        for r in data:
            for k in r:
                if str(k) not in keys:
                    keys.append(str(k))
        return {k: [r.get(k) for r in data] for k in keys}

    raise TypeError(
        f"Unsupported data type {type(data).__name__}. "
        "Pass a DataFrame, dict of columns, record dict, or list of dicts."
    )


def _from_native(data: Mapping, *, nested: str = "json", drop_none: bool = True, empty_fill: Optional[dict] = None) -> Prepared:
    ids = [str(i) for i in data["ids"]]
    if len(set(ids)) != len(ids):
        dupes = len(ids) - len(set(ids))
        raise ValueError(f"{dupes} duplicate id(s) in input. Chroma requires unique ids per request.")
    metas = data.get("metadatas")
    if metas is not None:
        metas = [sanitize_metadata(m or {}, nested=nested, drop_none=drop_none) for m in metas]
        metas = _fill_empty(metas, empty_fill)
    embs = data.get("embeddings")
    if embs is not None:
        embs = [[float(x) for x in e] for e in embs]
    docs = data.get("documents")
    return Prepared(ids, list(docs) if docs is not None else None, metas, embs)


def _fill_empty(metas: List[dict], fill: Optional[dict] = None) -> Optional[List[dict]]:
    """Chroma rejects empty metadata dicts. All-empty -> None; mixed -> placeholder."""
    if all(not m for m in metas):
        return None
    fill = DEFAULT_EMPTY_FILL if fill is None else fill
    return [m if m else dict(fill) for m in metas]


def prepare(
    data: Any,
    *,
    document_cols: Union[str, Sequence[str], None] = None,
    id_col: Optional[str] = None,
    metadata_cols: Optional[Sequence[str]] = None,
    embedding_col: Optional[str] = None,
    id_strategy: str = "uuid",          # "uuid" or "hash" (hash => re-runs are idempotent with upsert)
    doc_separator: str = " | ",
    nested: str = "json",               # "json" | "drop" | "error"
    drop_none: bool = True,
    empty_metadata_fill: Optional[dict] = None,
) -> Prepared:
    if isinstance(data, Mapping) and "ids" in data and ("documents" in data or "embeddings" in data):
        return _from_native(data, nested=nested, drop_none=drop_none, empty_fill=empty_metadata_fill)

    cols = _to_columns(data)
    n = len(next(iter(cols.values()))) if cols else 0
    if n == 0:
        return Prepared(ids=[])

    def need(name: str, role: str):
        if name not in cols:
            raise KeyError(f"{role} column {name!r} not found. Available: {list(cols)}")

    # ---- documents ----
    doc_list: Optional[List[str]] = None
    doc_set: set = set()
    if document_cols is not None:
        dcols = [document_cols] if isinstance(document_cols, str) else list(document_cols)
        for c in dcols:
            need(c, "document")
        doc_set = set(dcols)
        doc_list = []
        for i in range(n):
            if len(dcols) == 1:
                v = sanitize_value(cols[dcols[0]][i])
                doc_list.append("" if v is None else str(v))
            else:
                parts = []
                for c in dcols:
                    v = sanitize_value(cols[c][i])
                    if v is not None:
                        parts.append(f"{c}: {v}")
                doc_list.append(doc_separator.join(parts))

    # ---- embeddings ----
    emb_list = None
    if embedding_col is not None:
        need(embedding_col, "embedding")
        emb_list = []
        for i, e in enumerate(cols[embedding_col]):
            if e is None:
                raise ValueError(f"Row {i} has no embedding in column {embedding_col!r}")
            emb_list.append([float(x) for x in e])

    if doc_list is None and emb_list is None:
        raise ValueError("Provide document_cols and/or embedding_col so Chroma has something to index.")

    # ---- metadata ----
    excluded = doc_set | ({id_col} if id_col else set()) | ({embedding_col} if embedding_col else set())
    mcols = [c for c in cols if c not in excluded] if metadata_cols is None else list(metadata_cols)
    for c in mcols:
        need(c, "metadata")
    meta_list = None
    if mcols:
        meta_list = [
            sanitize_metadata({c: cols[c][i] for c in mcols}, nested=nested, drop_none=drop_none)
            for i in range(n)
        ]
        meta_list = _fill_empty(meta_list, empty_metadata_fill)

    # ---- ids ----
    if id_col is not None:
        need(id_col, "id")
        ids = []
        for i, v in enumerate(cols[id_col]):
            sv = sanitize_value(v)
            if sv is None:
                raise ValueError(f"Row {i} has a missing id in column {id_col!r}")
            ids.append(str(sv))
    elif id_strategy == "hash":
        ids = [
            hashlib.sha1(
                (doc_list[i] if doc_list is not None
                 else json.dumps({c: str(cols[c][i]) for c in cols}, sort_keys=True)).encode()
            ).hexdigest()
            for i in range(n)
        ]
    elif id_strategy == "uuid":
        ids = [str(uuid.uuid4()) for _ in range(n)]
    else:
        raise ValueError("id_strategy must be 'uuid' or 'hash'")

    if len(set(ids)) != len(ids):
        dupes = len(ids) - len(set(ids))
        raise ValueError(f"{dupes} duplicate id(s) in input. Chroma requires unique ids per request.")

    return Prepared(ids, doc_list, meta_list, emb_list)
