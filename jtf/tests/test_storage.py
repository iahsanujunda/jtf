import pytest
from google.api_core.exceptions import PreconditionFailed

from jtf.storage import AlreadyExists, GcsStore, LocalStore, open_store


def test_local_store_writes_once(tmp_path):
    store = LocalStore(tmp_path)
    url = store.put_new("a/b/file.csv", b"x", "text/csv")
    assert url.endswith("/a/b/file.csv")
    assert store.get("a/b/file.csv") == b"x"


def test_local_store_never_overwrites(tmp_path):
    store = LocalStore(tmp_path)
    store.put_new("file.csv", b"first", "text/csv")
    with pytest.raises(AlreadyExists):
        store.put_new("file.csv", b"second", "text/csv")
    assert store.get("file.csv") == b"first"


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket, self.name = bucket, name

    def upload_from_string(self, data, content_type, if_generation_match):
        assert if_generation_match == 0
        if self.name in self.bucket.objects:
            raise PreconditionFailed("exists")
        self.bucket.objects[self.name] = data

    def download_as_bytes(self):
        return self.bucket.objects[self.name]


class FakeBucket:
    def __init__(self):
        self.objects = {}

    def blob(self, name):
        return FakeBlob(self, name)


def test_gcs_store_uses_create_only_precondition():
    bucket = FakeBucket()
    store = GcsStore("raw-bucket", "prefix", bucket=bucket)
    assert store.put_new("x.csv", b"1", "text/csv") == "gs://raw-bucket/prefix/x.csv"
    assert bucket.objects == {"prefix/x.csv": b"1"}
    with pytest.raises(AlreadyExists):
        store.put_new("x.csv", b"2", "text/csv")


def test_open_store_parses_urls(tmp_path):
    assert isinstance(open_store(tmp_path.as_uri()), LocalStore)
    with pytest.raises(ValueError):
        open_store("s3://bucket")
