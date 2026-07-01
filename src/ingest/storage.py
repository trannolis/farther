"""S3-style storage contracts and a local adapter for demos/tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol


class ObjectStore(Protocol):
    """Small object-store surface used by the skeleton pipelines."""

    def put_bytes(self, key: str, body: bytes) -> None:
        ...

    def put_json(self, key: str, body: dict[str, Any]) -> None:
        ...

    def read_json(self, key: str) -> dict[str, Any]:
        ...

    def list_keys(self, prefix: str) -> list[str]:
        ...


class LocalObjectStore:
    """Filesystem-backed stand-in for S3 during local demos."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def put_bytes(self, key: str, body: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)

    def put_json(self, key: str, body: dict[str, Any]) -> None:
        payload = json.dumps(body, indent=2, sort_keys=True).encode("utf-8")
        self.put_bytes(key, payload)

    def read_json(self, key: str) -> dict[str, Any]:
        return json.loads((self.root / key).read_text(encoding="utf-8"))

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return [
            str(path.relative_to(self.root))
            for path in sorted(base.rglob("*"))
            if path.is_file()
        ]
