"""Shared HTTP client for all three ingest sources.

One place for the three things every source needs and none of them should
reimplement:

1. Retry with backoff on the transient failures (429, 5xx, connection resets,
   read timeouts) and *no* retry on the permanent ones (4xx other than 429) --
   retrying a 400 just multiplies a bug by `max_attempts`.
2. Honouring `Retry-After` when the server sends it. Socrata and ArcGIS both do
   under load. Guessing an exponential delay when the server has told you
   exactly how long to wait is how you get your token throttled harder.
3. A client-side minimum interval between requests, so we behave against
   anonymous Socrata (which throttles on IP, app token or not) and against a
   free ArcGIS FeatureServer we do not own.

Raw-bytes discipline: `get()` hands back the `requests.Response`, and callers
write `response.content` to bronze verbatim before parsing anything. Nothing in
this module decodes, normalises, or reserialises a payload.
"""

from __future__ import annotations

import hashlib
import logging
import os
import random
import time
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = logging.getLogger(__name__)

USER_AGENT = (
    "crash-to-contact/0.1 (take-home data engineering exercise; "
    "contact via repository owner)"
)

# Status codes worth trying again. 429 = throttled, 5xx = server-side wobble.
# 408/425 are rare but transient by definition.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

# Ceiling on any single sleep, including one the server asked for via
# Retry-After. A hostile or misconfigured header should not park a run for an
# hour; if the wait is genuinely that long the run should fail and be rescheduled.
MAX_SLEEP_SECONDS = 120.0


def _durable_rename(tmp: Path, dest: Path) -> None:
    """Rename and fsync the directory, so the rename itself survives a crash."""
    tmp.replace(dest)
    dir_fd = os.open(dest.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


class HttpError(RuntimeError):
    """A request failed and will not be retried."""

    def __init__(self, message: str, *, status: int | None = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


class RetryableHttpError(HttpError):
    """A request failed in a way that is worth trying again."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        body: str = "",
        retry_after: float | None = None,
    ):
        super().__init__(message, status=status, body=body)
        self.retry_after = retry_after


def parse_retry_after(value: str | None) -> float | None:
    """RFC 9110 Retry-After: either delta-seconds or an HTTP-date.

    Returns None for absent/unparseable values so the caller falls back to
    exponential backoff rather than trusting a malformed header.
    """
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


class HttpClient:
    """A retrying, politely-paced HTTP client over a single keep-alive session.

    Parameters
    ----------
    min_interval:
        Seconds to leave between the *start* of consecutive requests. This is
        the anonymous-throttle guard; Socrata's documented anonymous budget is
        shared per IP, so an app token raises the ceiling but does not remove
        the need to pace.
    max_attempts:
        Total attempts per request, not retries after the first.
    """

    def __init__(
        self,
        *,
        timeout: float = 60.0,
        max_attempts: int = 6,
        min_interval: float = 0.0,
        headers: Mapping[str, str] | None = None,
        user_agent: str = USER_AGENT,
        session: requests.Session | None = None,
    ) -> None:
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.min_interval = min_interval
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        if headers:
            self.session.headers.update(dict(headers))
        self._last_request_at: float = 0.0
        # Counters the ingest modules log at the end of a run; cheap evidence
        # that throttling was actually exercised rather than merely handled.
        self.stats: dict[str, int] = {"requests": 0, "retries": 0, "throttled": 0}

    # -- pacing ----------------------------------------------------------

    def _pace(self) -> None:
        if self.min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    # -- retry policy ----------------------------------------------------

    def _wait(self, retry_state) -> float:
        """Retry-After if the server sent one, exponential backoff otherwise."""
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, RetryableHttpError) and exc.retry_after is not None:
            # Add a little jitter on top of the server's number so a fleet of
            # workers does not resume in lockstep and re-trip the throttle.
            return min(exc.retry_after + random.uniform(0, 1.0), MAX_SLEEP_SECONDS)
        return wait_exponential_jitter(
            initial=1.0, max=MAX_SLEEP_SECONDS, exp_base=2.0, jitter=2.0
        )(retry_state)

    def _before_sleep(self, retry_state) -> None:
        self.stats["retries"] += 1
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, HttpError) and exc.status == 429:
            self.stats["throttled"] += 1
        log.warning(
            "retrying in %.1fs after attempt %d: %s",
            retry_state.next_action.sleep if retry_state.next_action else 0.0,
            retry_state.attempt_number,
            exc,
        )

    def _retrying(self) -> Retrying:
        return Retrying(
            stop=stop_after_attempt(self.max_attempts),
            wait=self._wait,
            retry=retry_if_exception_type(
                (RetryableHttpError, requests.exceptions.ConnectionError,
                 requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError)
            ),
            before_sleep=self._before_sleep,
            reraise=True,
        )

    # -- requests --------------------------------------------------------

    def _check(self, response: requests.Response) -> requests.Response:
        if response.status_code in RETRYABLE_STATUS:
            raise RetryableHttpError(
                f"HTTP {response.status_code} for {response.url}",
                status=response.status_code,
                body=response.text[:500],
                retry_after=parse_retry_after(response.headers.get("Retry-After")),
            )
        if response.status_code >= 400:
            raise HttpError(
                f"HTTP {response.status_code} for {response.url}",
                status=response.status_code,
                body=response.text[:500],
            )
        return response

    def _single_request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
        headers: Mapping[str, str] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        """Exactly one attempt. `download()` drives its own retry loop and must
        not nest one inside another, or 6 attempts becomes 36."""
        self._pace()
        self._last_request_at = time.monotonic()
        self.stats["requests"] += 1
        response = self.session.request(
            method,
            url,
            params=params,
            headers=dict(headers) if headers else None,
            timeout=self.timeout,
            stream=stream,
        )
        return self._check(response)

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
        headers: Mapping[str, str] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        for attempt in self._retrying():
            with attempt:
                return self._single_request(
                    method, url, params=params, headers=headers, stream=stream
                )
        raise AssertionError("unreachable: Retrying either returns or raises")

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def head(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("HEAD", url, **kwargs)

    def get_json(self, url: str, **kwargs: Any) -> Any:
        """Convenience for metadata probes only.

        Never use this on a payload destined for bronze: bronze gets the raw
        bytes off `get()`, and parsing happens only after those bytes are on
        disk. This is for `?f=json` layer descriptors and count-only queries.
        """
        return self.get(url, **kwargs).json()

    def download(
        self,
        url: str,
        dest: Path,
        *,
        chunk_size: int = 1 << 18,  # 256KB: resume granularity, not throughput
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Stream a large file to `dest`, hashing as it goes.

        The retry scope covers the *body*, not just the response headers. The
        first version of this wrapped only the request, and a FARS download
        stalled 26MB into a 32MB file and killed the run: `iter_content` raised
        a read timeout from outside the retry loop, where nothing was watching.
        A 34MB transfer from a CDN that goes quiet mid-body is the normal case
        this has to survive, not an edge case.

        A retry resumes with `Range` rather than starting over, and pairs it
        with `If-Range` carrying the previous attempt's validator, so a file
        that is revised mid-download makes the server send the whole new entity
        (200) instead of splicing two versions into one corrupt zip -- which for
        a source that is silently revised in place is a real hazard, not a
        theoretical one. Any pre-existing `.part` is discarded first: it could
        be from an older revision, and there is no validator to check it with.

        Writes to `.part` and renames only after a complete, fsynced,
        length-checked body, so an interrupted download can never be mistaken
        for a good one by a later run.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        tmp.unlink(missing_ok=True)

        validator: str | None = None
        expected: int | None = None

        for attempt in self._retrying():
            with attempt:
                offset = tmp.stat().st_size if tmp.exists() else 0
                request_headers = dict(headers or {})
                if offset and validator:
                    request_headers["Range"] = f"bytes={offset}-"
                    request_headers["If-Range"] = validator
                elif offset:
                    offset = 0  # nothing to validate a resume against

                response = self._single_request(
                    "GET", url, headers=request_headers, stream=True
                )
                validator = response.headers.get("ETag") or response.headers.get(
                    "Last-Modified"
                )
                resumed = response.status_code == 206
                if offset and not resumed:
                    # Server ignored Range, or If-Range failed because the file
                    # changed. Either way this is a full body: start again.
                    offset = 0
                if not offset:
                    tmp.unlink(missing_ok=True)

                digest = hashlib.sha256()
                if offset:
                    with tmp.open("rb") as fh:
                        for chunk in iter(lambda: fh.read(chunk_size), b""):
                            digest.update(chunk)

                content_range = response.headers.get("Content-Range")
                if resumed and content_range and "/" in content_range:
                    total = content_range.rsplit("/", 1)[1]
                    expected = int(total) if total.isdigit() else expected
                elif response.headers.get("Content-Length") is not None:
                    expected = offset + int(response.headers["Content-Length"])

                size = offset
                with response:
                    with tmp.open("ab" if offset else "wb") as fh:
                        try:
                            for chunk in response.iter_content(chunk_size=chunk_size):
                                if not chunk:
                                    continue
                                digest.update(chunk)
                                size += len(chunk)
                                fh.write(chunk)
                        except (requests.exceptions.RequestException, OSError) as exc:
                            fh.flush()
                            os.fsync(fh.fileno())
                            # Keep the bytes: the next attempt resumes from here.
                            raise RetryableHttpError(
                                f"stream interrupted at {size} bytes of "
                                f"{expected or '?'}: {exc}"
                            ) from exc
                        fh.flush()
                        os.fsync(fh.fileno())

                if expected is not None and size != expected:
                    raise RetryableHttpError(
                        f"short body: got {size} bytes, expected {expected}"
                    )

                _durable_rename(tmp, dest)
                return {
                    "path": dest,
                    "sha256": digest.hexdigest(),
                    "bytes": size,
                    "last_modified": response.headers.get("Last-Modified"),
                    "etag": response.headers.get("ETag"),
                    "status": response.status_code,
                }
        raise AssertionError("unreachable: Retrying either returns or raises")

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
