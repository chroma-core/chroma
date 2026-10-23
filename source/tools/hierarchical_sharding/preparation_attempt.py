"""Preserve preparation attempts and atomically point to the latest report."""
from pathlib import Path
from .tree_manifest import _atomic_write,canonical_bytes,artifact


def new_attempt(root):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    numbers=[int(p.name.split('-')[-1]) for p in root.glob('attempt-*') if p.is_dir()]
    path=root/f'attempt-{max(numbers,default=-1)+1:04d}'
    path.mkdir(exist_ok=False)
    return path


def save_attempt(path,report):
    path=Path(path);_atomic_write(path/'run.json',canonical_bytes(report))
    _atomic_write(path.parent/'latest.json',canonical_bytes(dict(attempt=path.name,report=artifact(path/'run.json'))))
