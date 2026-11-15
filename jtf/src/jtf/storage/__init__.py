"""Raw and model files addressed by URL, so the rest of the code never imports a cloud SDK.

`gs://bucket/prefix` stores in Cloud Storage, `file:///path` on disk. Writes are create-only:
writing to a path that already exists raises `AlreadyExists`, so raw files can never be
overwritten, not even by a bug.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse


class AlreadyExists(Exception):
    """The path is already taken; raw files are never overwritten."""


class Store(Protocol):
    url: str

    def put_new(self, path: str, data: bytes, content_type: str) -> str:
        """Write `data` at `path` only if nothing is there yet; return the full URL."""
        ...

    def get(self, path: str) -> bytes: ...


class LocalStore:
    def __init__(self, root: Path):
        self.root = root
        self.url = root.as_uri()

    def put_new(self, path: str, data: bytes, content_type: str) -> str:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with target.open("xb") as f:
                f.write(data)
        except FileExistsError as e:
            raise AlreadyExists(target.as_uri()) from e
        return target.as_uri()

    def get(self, path: str) -> bytes:
        return (self.root / path).read_bytes()


class GcsStore:
    def __init__(self, bucket_name: str, prefix: str = "", bucket=None):
        if bucket is None:
            from google.cloud import storage

            bucket = storage.Client().bucket(bucket_name)
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.url = f"gs://{bucket_name}" + (f"/{self.prefix}" if self.prefix else "")

    def _name(self, path: str) -> str:
        return f"{self.prefix}/{path}" if self.prefix else path

    def put_new(self, path: str, data: bytes, content_type: str) -> str:
        from google.api_core.exceptions import PreconditionFailed

        name = self._name(path)
        try:
            # if_generation_match=0: succeed only if no object with this name exists.
            self.bucket.blob(name).upload_from_string(
                data, content_type=content_type, if_generation_match=0
            )
        except PreconditionFailed as e:
            raise AlreadyExists(f"{self.url}/{path}") from e
        return f"{self.url}/{path}"

    def get(self, path: str) -> bytes:
        return self.bucket.blob(self._name(path)).download_as_bytes()


def open_store(url: str) -> Store:
    parsed = urlparse(url)
    if parsed.scheme == "gs":
        return GcsStore(parsed.netloc, parsed.path)
    if parsed.scheme == "file":
        return LocalStore(Path(parsed.path))
    raise ValueError(f"Unsupported store URL: {url!r} (use gs:// or file://)")
