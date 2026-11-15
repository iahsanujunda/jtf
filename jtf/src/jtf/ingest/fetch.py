"""Fetch one source and store every file unchanged, with a metadata sidecar.

The fetch time recorded here becomes `known_at`, so this is the one place that reads the clock.

Three modes:
- daily: the scheduled capture; always downloads, so every day records what was there
- initial: the one-off history load; skips what is already stored, so it can stop and resume
- recapture: checks every file for rewrites with `If-Modified-Since`; stores only what changed
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from jtf import __version__
from jtf.ingest.sources import JST, FetchItem, GapCheck, RetryPolicy, Source
from jtf.storage import Store

log = logging.getLogger(__name__)

USER_AGENT = f"jtf/{__version__} (+https://github.com/iahsanujunda/jtf)"
HEALTHCHECKS_PING = "https://hc-ping.com"
MODES = ("daily", "initial", "recapture")
# Cookies are session noise from the source's load balancer, not data.
DROPPED_HEADERS = {"set-cookie"}


class QuotaExhausted(Exception):
    """The source refused more requests for now (HTTP 429); a later run resumes."""


@dataclass
class CaptureResult:
    source: str
    mode: str = "daily"
    stored: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    calls_spent: float = 0.0
    stopped: str | None = None

    @property
    def ok(self) -> bool:
        return not self.failures

    def summary(self) -> str:
        lines = [
            f"source={self.source} mode={self.mode} stored={len(self.stored)} "
            f"unchanged={len(self.unchanged)} skipped={len(self.skipped)} gaps={len(self.gaps)} "
            f"failed={len(self.failures)} calls={self.calls_spent:g}"
        ]
        if self.stopped:
            lines.append(f"STOPPED EARLY: {self.stopped}; run again to resume")
        lines += [f"FAILED {f}" for f in self.failures]
        lines += [f"stored {s}" for s in self.stored]
        return "\n".join(lines)


@dataclass(frozen=True)
class Fetched:
    stored: str | None
    status: int
    cost: float
    gap: bool = False


class Healthcheck:
    """Pings healthchecks.io by slug. A failed ping is logged, never allowed to fail the capture."""

    def __init__(self, url: str | None, client: httpx.Client | None = None):
        self.url = url.rstrip("/") if url else None
        self.client = client or httpx.Client(timeout=10)

    @classmethod
    def for_source(
        cls,
        ping_key: str | None,
        source: Source,
        mode: str = "daily",
        client: httpx.Client | None = None,
    ) -> Healthcheck:
        """Scheduled runs ping their own check; one-off initial loads are watched by hand."""
        if not ping_key or mode == "initial":
            return cls(None, client)
        slug = source.slug if mode == "daily" else f"{source.slug}-{mode}"
        return cls(f"{HEALTHCHECKS_PING}/{ping_key}/jtf-{slug}", client)

    def _ping(self, suffix: str, body: str = "") -> None:
        if not self.url:
            return
        try:
            # create=1: a check that doesn't exist yet is created on its first ping.
            response = self.client.post(
                self.url + suffix, params={"create": "1"}, content=body.encode()[:100_000]
            )
            if response.status_code >= 400:
                log.warning("healthcheck ping %s rejected: %s", suffix or "/", response.status_code)
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
    client: httpx.Client,
    item: FetchItem,
    headers: dict[str, str],
    retry: RetryPolicy,
    sleep: Callable[[float], None],
) -> httpx.Response:
    for attempt, delay in enumerate((*retry.delays_s, None), start=1):
        try:
            response = client.request(
                item.method,
                item.url,
                params=item.params or None,
                data=item.form or None,
                headers=headers or None,
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
    if filename.endswith(".zip"):
        return "application/zip"
    return "application/octet-stream"


def fetch_item(
    item: FetchItem,
    *,
    store: Store,
    client: httpx.Client,
    retry: RetryPolicy,
    now: Callable[[], datetime],
    load_type: str = "daily",
    if_modified_since: str | None = None,
    is_gap: GapCheck | None = None,
    image_digest: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Fetched:
    headers = dict(item.headers)
    if if_modified_since:
        headers["If-Modified-Since"] = if_modified_since
    response = _request(client, item, headers, retry, sleep)
    cost = float(response.headers.get("x-ratelimit-cost", 0) or 0)
    if response.status_code == 304:
        return Fetched(None, 304, cost)
    if response.status_code == 429:
        raise QuotaExhausted(f"{item.url} answered 429: {response.text[:200]}")
    gap = bool(is_gap and is_gap(response.status_code, response.content))
    if not gap:
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
        "if_modified_since": if_modified_since,
        "fetched_at": fetched_at.isoformat(),
        "fetched_at_jst": fetched_jst.isoformat(),
        "extraction_date": extraction_date,
        "http_status": response.status_code,
        "gap": gap,
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
    return Fetched(stored, response.status_code, cost, gap)


def stored_files(store: Store, source: str) -> dict[str, list[str]]:
    """Filename -> stored paths, oldest first, from paths like `src/date/20261115T010031Z_name`."""
    index: dict[str, list[str]] = defaultdict(list)
    for path in store.list(f"{source}/"):
        if path.endswith(".meta.json"):
            continue
        _timestamp, _, filename = path.rsplit("/", 1)[-1].partition("_")
        index[filename].append(path)
    for paths in index.values():
        paths.sort(key=lambda p: p.rsplit("/", 1)[-1])
    return dict(index)


def _last_modified(store: Store, path: str) -> str | None:
    meta = json.loads(store.get(f"{path}.meta.json"))
    headers = {k.lower(): v for k, v in meta.get("response_headers", {}).items()}
    return headers.get("last-modified")


def run_source(
    source: Source,
    *,
    mode: str = "daily",
    store: Store,
    client: httpx.Client | None = None,
    healthcheck: Healthcheck | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    image_digest: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
    max_calls: float | None = None,
) -> CaptureResult:
    """Fetch one source's items for a mode. Success only if no item failed."""
    if mode not in MODES:
        raise ValueError(f"Unknown mode {mode!r}; expected one of {MODES}")
    make_items = source.items_for(mode)
    if make_items is None:
        raise ValueError(f"{source.name} has no {mode} items; its modes are {source.modes}")
    items = make_items(now())
    budget = max_calls if max_calls is not None else source.call_budget
    index = stored_files(store, source.name) if mode != "daily" else {}
    client = client or client_for(source.retry)
    healthcheck = healthcheck or Healthcheck(None)
    result = CaptureResult(source.name, mode)

    healthcheck.start()
    last_cost = 0.0
    for position, item in enumerate(items):
        if mode == "initial" and item.filename in index:
            result.skipped.append(item.filename)
            continue
        if budget is not None and result.calls_spent + max(last_cost, 1.0) > budget:
            result.stopped = f"call budget of {budget:g} reached"
            break
        previous = index.get(item.filename) if mode == "recapture" else None
        try:
            fetched = fetch_item(
                item,
                store=store,
                client=client,
                retry=source.retry,
                now=now,
                load_type=mode,
                if_modified_since=_last_modified(store, previous[-1]) if previous else None,
                is_gap=source.is_gap,
                image_digest=image_digest,
                sleep=sleep,
            )
            result.calls_spent += fetched.cost
            last_cost = fetched.cost
            if fetched.gap:
                result.gaps.append(item.filename)
            elif fetched.stored:
                result.stored.append(fetched.stored)
            else:
                result.unchanged.append(item.filename)
        except QuotaExhausted as e:
            result.stopped = str(e)
            break
        except Exception as e:  # one failed file must not stop the source's other files
            log.exception("failed to capture %s", item.url)
            result.failures.append(f"{item.source}/{item.filename}: {type(e).__name__}: {e}")
        if position < len(items) - 1:
            sleep(source.pacing.wait_after(last_cost))

    if result.ok:
        healthcheck.success(result.summary())
    else:
        healthcheck.fail(result.summary())
    return result
