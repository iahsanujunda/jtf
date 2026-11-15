"""Fetch one source and store every file unchanged, with a metadata sidecar.

The fetch time recorded here becomes `known_at`, so this is the one place that reads the clock.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from jtf import __version__
from jtf.ingest.sources import JST, FetchItem, RetryPolicy, Source
from jtf.storage import Store

log = logging.getLogger(__name__)

USER_AGENT = f"jtf/{__version__} (+https://github.com/iahsanujunda/jtf)"
HEALTHCHECKS_PING = "https://hc-ping.com"
# Cookies are session noise from the source's load balancer, not data.
DROPPED_HEADERS = {"set-cookie"}


@dataclass
class CaptureResult:
    source: str
    stored: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures

    def summary(self) -> str:
        lines = [f"source={self.source} stored={len(self.stored)} failed={len(self.failures)}"]
        lines += [f"FAILED {f}" for f in self.failures]
        lines += [f"stored {s}" for s in self.stored]
        return "\n".join(lines)


class Healthcheck:
    """Pings healthchecks.io by slug. A failed ping is logged, never allowed to fail the capture."""

    def __init__(self, url: str | None, client: httpx.Client | None = None):
        self.url = url.rstrip("/") if url else None
        self.client = client or httpx.Client(timeout=10)

    @classmethod
    def for_source(
        cls, ping_key: str | None, source: Source, client: httpx.Client | None = None
    ) -> Healthcheck:
        url = f"{HEALTHCHECKS_PING}/{ping_key}/jtf-{source.slug}" if ping_key else None
        return cls(url, client)

    def _ping(self, suffix: str, body: str = "") -> None:
        if not self.url:
            return
        try:
            # create=1: a check that doesn't exist yet is created on its first ping.
            self.client.post(
                self.url + suffix, params={"create": "1"}, content=body.encode()[:100_000]
            )
        except httpx.HTTPError as e:
            log.warning("healthcheck ping %s failed: %s", suffix or "/", e)

    def start(self) -> None:
        self._ping("/start")

    def success(self, body: str) -> None:
        self._ping("", body)

    def fail(self, body: str) -> None:
        self._ping("/fail", body)


def client_for(retry: RetryPolicy) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(retry.timeout_s, connect=retry.connect_timeout_s),
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )


def _request(
    client: httpx.Client, item: FetchItem, retry: RetryPolicy, sleep: Callable[[float], None]
) -> httpx.Response:
    for attempt, delay in enumerate((*retry.delays_s, None), start=1):
        try:
            response = client.request(
                item.method,
                item.url,
                params=item.params or None,
                data=item.form or None,
                headers=item.headers or None,
            )
            if response.status_code < 500 or delay is None:
                return response
            log.warning("%s returned %s (attempt %d)", item.url, response.status_code, attempt)
        except httpx.TransportError as e:
            if delay is None:
                raise
            log.warning("%s: %s (attempt %d/%d)", item.url, e, attempt, retry.attempts)
        sleep(delay)
    raise AssertionError("unreachable")


def _content_type(filename: str) -> str:
    if filename.endswith(".csv"):
        return "text/csv"
    if filename.endswith(".json"):
        return "application/json"
    return "application/octet-stream"


def fetch_item(
    item: FetchItem,
    *,
    store: Store,
    client: httpx.Client,
    retry: RetryPolicy,
    now: Callable[[], datetime],
    load_type: str = "daily",
    image_digest: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    response = _request(client, item, retry, sleep)
    response.raise_for_status()
    fetched_at = now().astimezone(UTC)
    fetched_jst = fetched_at.astimezone(JST)
    extraction_date = fetched_jst.date().isoformat()
    path = f"{item.source}/{extraction_date}/{fetched_at:%Y%m%dT%H%M%SZ}_{item.filename}"
    body = response.content

    stored = store.put_new(path, body, _content_type(item.filename))
    meta = {
        "source": item.source,
        "filename": item.filename,
        "load_type": load_type,
        "url": item.url,
        "method": item.method,
        "params": item.params,
        "form": item.form,
        "fetched_at": fetched_at.isoformat(),
        "fetched_at_jst": fetched_jst.isoformat(),
        "extraction_date": extraction_date,
        "http_status": response.status_code,
        "response_headers": {
            k: v for k, v in response.headers.items() if k.lower() not in DROPPED_HEADERS
        },
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "jtf_version": __version__,
        "image_digest": image_digest,
    }
    store.put_new(
        f"{path}.meta.json",
        json.dumps(meta, ensure_ascii=False, indent=2).encode(),
        "application/json",
    )
    return stored


def run_source(
    source: Source,
    *,
    store: Store,
    client: httpx.Client | None = None,
    healthcheck: Healthcheck | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    image_digest: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CaptureResult:
    """Fetch every file of one source. Success only if every file was stored."""
    client = client or client_for(source.retry)
    healthcheck = healthcheck or Healthcheck(None)
    result = CaptureResult(source.name)
    healthcheck.start()
    for item in source.items(now().astimezone(JST)):
        try:
            result.stored.append(
                fetch_item(
                    item,
                    store=store,
                    client=client,
                    retry=source.retry,
                    now=now,
                    image_digest=image_digest,
                    sleep=sleep,
                )
            )
        except Exception as e:  # one failed file must not stop the source's other files
            log.exception("failed to capture %s", item.url)
            result.failures.append(f"{item.source}/{item.filename}: {type(e).__name__}: {e}")
    if result.ok:
        healthcheck.success(result.summary())
    else:
        healthcheck.fail(result.summary())
    return result
