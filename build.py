#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "click",
#     "hishel<1",
#     "httpx",
#     "pyarrow",
#     "pyrate-limiter>=4",
#     "python-dotenv",
#     "tenacity",
#     "tqdm",
# ]
# ///
"""Build the Europeana Open Newspapers dataset.

Harvests openly licensed newspapers with OCR full text from Europeana's
Fulltext Search, Entity and IIIF APIs and writes items.parquet,
enrichments.parquet, entities.parquet and pages.parquet.

The build runs three phases (items -> entities -> pages), each a `Phase`
subclass orchestrated by `BuildPipeline`. Every fan-out goes through the same
worker/queue machinery: jobs drain through a `WorkerPool` whose handlers emit
result messages into a bounded queue, and a single writer coroutine per phase
owns all disk I/O (part files, done-lists, checkpoint saves).
"""

import asyncio
import hashlib
import json
import os
import re
import shutil
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
from functools import cached_property
from pathlib import Path
from time import perf_counter
from typing import Any, ClassVar
from urllib.parse import urlsplit

import click
import hishel
import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyrate_limiter import Duration, limiter_factory
from pyrate_limiter.extras.httpx_limiter import AsyncRateLimiterTransport
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class PhaseName(StrEnum):
    ITEMS = "items"
    ENTITIES = "entities"
    PAGES = "pages"


class SampleStrategy(StrEnum):
    PROPORTIONAL = "proportional"  # each dataset's share mirrors the corpus
    BALANCED = "balanced"  # every dataset gets an equal share


class Fmt:
    """Human-readable console formatting."""

    @staticmethod
    def count(n: int) -> str:
        return f"{n:,}"

    @staticmethod
    def duration(seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.1f}s"
        minutes, secs = divmod(round(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours}h {minutes:02d}m" if hours else f"{minutes}m {secs:02d}s"

    @staticmethod
    def size(n_bytes: int) -> str:
        size = float(n_bytes)
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024:
                return f"{size:,.0f} {unit}" if unit == "B" else f"{size:,.1f} {unit}"
            size /= 1024
        return f"{size:,.1f} TB"


class RetryableStatus(Exception):
    """HTTP status worth retrying (429/5xx)."""


class NonRetryableStatus(Exception):
    """HTTP status that will not improve with retries (e.g. 404)."""


class InvalidJson(Exception):
    """A 200 response whose body failed to parse as JSON."""


@dataclass(frozen=True)
class RetryPolicy:
    """Retry envelope for every Europeana request.

    Europeana returns 502s under load that can last minutes. A cursor chain
    cannot skip a page, so a give-up costs the whole partition; these attempts
    span roughly four minutes of capped exponential backoff instead, with
    jitter so that hundreds of concurrent chains hit by the same outage do not
    retry in lockstep and re-spike the server as it recovers.
    """

    attempts: int = 8
    initial: float = 1.0  # doubled per attempt: 2, 4, 8, ... capped at `cap`
    cap: float = 60.0
    jitter: float = 15.0  # uniform seconds added on top of each wait
    retryable_statuses: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    def retrying(
        self, before_sleep: Callable[[RetryCallState], None] | None = None
    ) -> AsyncRetrying:
        """A fresh retry loop per request (instances are cheap, and sharing one
        across concurrent calls would share its iteration statistics).

        The predicate deliberately names `httpx.TransportError`, not
        `httpx.HTTPError`: the latter includes `HTTPStatusError`, which would
        make any stray `raise_for_status()` retry 404s for four minutes each.
        """
        return AsyncRetrying(
            retry=retry_if_exception_type(
                (httpx.TransportError, RetryableStatus, InvalidJson)
            ),
            wait=wait_exponential_jitter(
                initial=self.initial, max=self.cap, jitter=self.jitter
            ),
            stop=stop_after_attempt(self.attempts),
            before_sleep=before_sleep,
            reraise=True,
        )


@dataclass(frozen=True)
class Settings:
    """Every tunable of the build, constructed once in `main` and passed around.

    Frozen, so no phase can mutate shared configuration mid-run.
    """

    # The corpus is defined as date-bearing: every item must carry a
    # dcterms:issued date. This drops a single dateless record from ~995k, and
    # in exchange the whole dataset is partitionable and filterable by year.
    #
    # IMPORTANT: Europeana ORs multiple qf values on the SAME field (different
    # fields are ANDed), so this must never be sent alongside a year range on
    # proxy_dcterms_issued -- "has a date OR is from 1873" matches the whole
    # corpus. It is used on its own, to count the date-bearing corpus; a year
    # range already implies the field is present.
    ISSUED_PRESENT: ClassVar[str] = "proxy_dcterms_issued:[* TO *]"

    api_key: str | None
    output_dir: Path
    cache_dir: Path
    phases: tuple[PhaseName, ...]
    sample_size: int
    sample_strategy: SampleStrategy
    rate_limit: int  # requests per second, across all APIs
    max_partitions: int  # testing: harvest only the first N year partitions (0 = all)
    max_requests: int  # testing: cap search requests per partition (0 = unlimited)

    # The Fulltext Search API, not record/v2/search.json. Only records ingested
    # into Europeana's Fulltext API are served here, and only those get IIIF
    # AnnotationPages (i.e. the pages phase). The Search API's
    # text_fulltext=true flag is a different, largely disjoint set: it marks
    # records whose media file is text-searchable (e.g. a PDF with a text
    # layer), whose OCR was never ingested and has no annotations.
    fulltext_search_api: str = "https://api.europeana.eu/fulltext/search.json"
    entity_api: str = "https://api.europeana.eu/entity"
    iiif_presentation: str = "https://iiif.europeana.eu/presentation"
    data_europeana_prefix: str = "http://data.europeana.eu/"
    item_uri_prefix: str = "http://data.europeana.eu/item"

    # Publication years spanned by the corpus (upper bound exclusive). Verified
    # against the API: nothing sits outside 1600-1950. ItemsPhase asserts that
    # the per-year counts still sum to totalResults, so a corpus that grows
    # past these bounds fails loudly rather than being silently truncated.
    year_min: int = 1600
    year_max: int = 1950

    # The theme=newspaper query also returns a handful of records from
    # collections that are not newspaper collections at all (e.g.
    # 135_Ag_EU_1989_Germany, a crowdsourced 1989 photo archive whose items
    # carry dc_type "Newspaper" but have no OCR). Every genuine newspaper
    # dataset has "Newspapers" in its edm_datasetName, so require it.
    newspaper_dataset_substring: str = "newspapers"

    # In-flight requests. Each Search API response takes ~0.75s, so a cursor
    # chain manages only ~1.3 req/s on its own; enough chains must be in flight
    # for rate_limit to be the binding constraint rather than this.
    concurrency: int = 24
    items_flush_every: int = 200  # search requests per part-file flush
    entity_batch: int = 500  # resolved entities per part/checkpoint flush
    pages_batch: int = 25  # harvested items per part/checkpoint flush
    pages_split_bytes: int = 1 << 30  # ~1 GB shards for pages.parquet
    retry: RetryPolicy = RetryPolicy()

    @classmethod
    def from_cli(cls, **cli: Any) -> "Settings":
        """CLI options plus the environment's API key."""
        return cls(api_key=os.environ.get("EUROPEANA_API_KEY"), **cli)

    @property
    def parts_dir(self) -> Path:
        return self.output_dir / "parts"

    def base_search_params(self) -> dict[str, Any]:
        """Base Fulltext Search query; callers append exactly one more qf."""
        return {
            "query": "*",
            "reusability": "open",
            "qf": ["TYPE:TEXT"],  # list -> repeated qf params
            "theme": "newspaper",
            "rows": "100",  # the server caps rows at 100, silently
            "profile": "rich",  # the only profile with the enrichment fields
        }

    @staticmethod
    def year_qf(year: int) -> str:
        """Half-open range for one year: [Y TO Y+1}, so years cannot overlap."""
        return f"proxy_dcterms_issued:[{year} TO {year + 1}}}"


class Schemas:
    """The four output Parquet schemas -- the dataset's compatibility contract."""

    ITEMS: ClassVar[pa.Schema] = pa.schema(
        [
            ("item_id", pa.string()),
            ("language", pa.string()),
            ("country", pa.string()),
            # Derived from the harvest partition, not from the payload: the
            # Search API can filter on proxy_dcterms_issued but never returns
            # it, on any profile.
            ("year_issued", pa.int16()),
            # Parsed out of dc_title, which ends in the issue date for every item.
            ("date_issued", pa.date32()),
            ("dataset_name", pa.string()),
            ("europeana_url", pa.string()),
            ("manifest_url", pa.string()),
            ("dc_title", pa.string()),
            ("dc_description", pa.string()),
            ("dc_type", pa.string()),
            ("dc_type_en", pa.list_(pa.string())),
            ("dc_subject_en", pa.list_(pa.string())),
            ("dc_creator_en", pa.list_(pa.string())),
            ("enriched_concepts", pa.string()),
            ("enriched_agents", pa.string()),
            ("enriched_places", pa.string()),
            ("enriched_timespans", pa.string()),
            ("data_provider", pa.string()),
            ("image_rights", pa.string()),
            ("theme", pa.string()),
            ("provider", pa.string()),
        ]
    )

    PAGES: ClassVar[pa.Schema] = pa.schema(
        [
            ("item_id", pa.string()),
            ("page_number", pa.int16()),
            ("page_id", pa.string()),
            ("language", pa.string()),
            ("text", pa.string()),
            ("annotations", pa.string()),
            ("image_url", pa.string()),
            ("image_mime_type", pa.string()),
            ("image_width", pa.int32()),
            ("image_height", pa.int32()),
            ("text_length", pa.int32()),
            ("text_rights", pa.string()),
        ]
    )

    ENRICHMENTS: ClassVar[pa.Schema] = pa.schema(
        [
            ("item_id", pa.string()),
            ("entity_uri", pa.string()),
            ("entity_class", pa.string()),
            ("source_property", pa.string()),
        ]
    )

    ENTITIES: ClassVar[pa.Schema] = pa.schema(
        [
            ("entity_uri", pa.string()),
            ("entity_class", pa.string()),
            ("field", pa.string()),
            ("value", pa.string()),
            ("language", pa.string()),
        ]
    )


# ---------------------------------------------------------------------------
# State and storage
# ---------------------------------------------------------------------------


class ErrorLog:
    """Append-only errors.log: every skipped record, one tab-separated line.

    Silent drops used to vanish without trace; the line count is published into
    metadata.json, and a number that jumps unexpectedly is the signal that a
    filter has started catching more than intended.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def log(self, phase: str, record_id: str, message: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(f"{stamp}\t{phase}\t{record_id}\t{message}\n")

    def count(self) -> int:
        if not self.path.exists():
            return 0
        with self.path.open(encoding="utf-8") as fh:
            return sum(1 for _ in fh)


@dataclass
class PartitionState:
    """Resume state for one year's cursor chain.

    `cursor` and `requests` describe the same point in the chain and are only
    ever persisted together, at flush time: advancing `requests` per-request
    would leave it ahead of the cursor after a crash, and the replayed requests
    would then be counted twice.
    """

    count: int
    cursor: str | None = "*"
    requests: int = 0
    part: int = 0
    done: bool = False


@dataclass
class ItemsState:
    partitions: dict[str, PartitionState] = field(default_factory=dict)
    done: bool = False
    finalized: bool = False
    skipped_non_newspaper: int = 0


@dataclass
class EntitiesState:
    part: int = 0
    finalized: bool = False
    post_done: bool = False


@dataclass
class PagesState:
    part: int = 0
    finalized: bool = False


@dataclass
class Checkpoint:
    """checkpoint.json: typed resume state, written atomically.

    A phase marked `finalized` is skipped on re-run, forever -- which is why
    nothing may set it until the phase is provably complete.
    """

    path: Path
    items: ItemsState = field(default_factory=ItemsState)
    entities: EntitiesState = field(default_factory=EntitiesState)
    pages: PagesState = field(default_factory=PagesState)

    @classmethod
    def load(cls, path: Path) -> "Checkpoint":
        if not path.exists():
            return cls(path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        items_raw = raw.get("items", {})
        return cls(
            path,
            items=ItemsState(
                partitions={
                    year: PartitionState(**pstate)
                    for year, pstate in items_raw.get("partitions", {}).items()
                },
                done=items_raw.get("done", False),
                finalized=items_raw.get("finalized", False),
                skipped_non_newspaper=items_raw.get("skipped_non_newspaper", 0),
            ),
            entities=EntitiesState(**raw.get("entities", {})),
            pages=PagesState(**raw.get("pages", {})),
        )

    def save(self) -> None:
        payload = {
            "items": asdict(self.items),
            "entities": asdict(self.entities),
            "pages": asdict(self.pages),
        }
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)


class MetadataFile:
    """metadata.json: harvest date, endpoints, per-phase counts, skip count."""

    def __init__(self, path: Path, settings: Settings, errors: ErrorLog) -> None:
        self._path = path
        self._settings = settings
        self._errors = errors

    def update(self, section: str, counts: dict[str, int]) -> None:
        meta = (
            json.loads(self._path.read_text(encoding="utf-8"))
            if self._path.exists()
            else {}
        )
        meta["harvest_date"] = datetime.now(timezone.utc).isoformat()
        meta["endpoints"] = {
            "fulltext_search_api": self._settings.fulltext_search_api,
            "entity_api": self._settings.entity_api + "/{type}/{id}",
            "iiif_presentation_api": self._settings.iiif_presentation
            + "/{record_id}/manifest?format=3",
            "iiif_fulltext_api": "{annopage_url}?profile=text",
        }
        meta.setdefault("counts", {})[section] = counts
        meta["records_skipped"] = self._errors.count()
        tmp = self._path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self._path)


class ParquetStore:
    """All Parquet and sidecar file I/O: parts in parts/, merged outputs beside.

    Merged outputs are written to a .tmp and moved into place, so a crash never
    leaves a truncated final file behind.
    """

    def __init__(self, output_dir: Path, parts_dir: Path) -> None:
        self.output_dir = output_dir
        self.parts_dir = parts_dir

    # -- part files ---------------------------------------------------------

    def write_part(self, name: str, rows: list[dict], schema: pa.Schema) -> None:
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, self.parts_dir / name, compression="snappy")

    def parts(self, pattern: str) -> list[Path]:
        return sorted(self.parts_dir.glob(pattern))

    # -- merged outputs -----------------------------------------------------

    @contextmanager
    def open_output(self, name: str, schema: pa.Schema) -> Iterator[pq.ParquetWriter]:
        out = self.output_dir / name
        tmp = out.with_suffix(".parquet.tmp")
        writer = pq.ParquetWriter(tmp, schema, compression="snappy")
        try:
            yield writer
        finally:
            writer.close()
        os.replace(tmp, out)  # skipped if the body raised: no partial outputs

    def merge(self, pattern: str, out_name: str, schema: pa.Schema) -> int:
        """Stream part files row-group-by-row-group into one Parquet file."""
        parts = self.parts(pattern)
        n_rows = 0
        with self.open_output(out_name, schema) as writer:
            if not parts:
                writer.write_table(pa.Table.from_pylist([], schema=schema))
            for part in parts:
                pf = pq.ParquetFile(part)
                for rg in range(pf.num_row_groups):
                    table = pf.read_row_group(rg)
                    if table.num_rows:
                        writer.write_table(table)
                        n_rows += table.num_rows
        return n_rows

    def merge_sharded(
        self,
        pattern: str,
        base: str,
        schema: pa.Schema,
        split_bytes: int,
        distinct_column: str,
    ) -> tuple[int, int]:
        """Merge parts into shards of roughly `split_bytes` each.

        Shards are named `{base}.parquet`, `{base}_001.parquet`, ... Returns
        (rows written, distinct values seen in `distinct_column`).
        """
        parts = self.parts(pattern)
        distinct: set[str] = set()
        n_rows = 0
        shard_index = 0
        writer: pq.ParquetWriter | None = None
        tmp_path: Path | None = None
        bytes_in_shard = 0

        def shard_path(index: int) -> Path:
            name = f"{base}.parquet" if index == 0 else f"{base}_{index:03d}.parquet"
            return self.output_dir / name

        def open_writer() -> None:
            nonlocal writer, tmp_path
            tmp_path = shard_path(shard_index).with_suffix(".parquet.tmp")
            writer = pq.ParquetWriter(tmp_path, schema, compression="snappy")

        def close_writer() -> None:
            nonlocal writer, shard_index, bytes_in_shard
            if writer is not None:
                writer.close()
                os.replace(tmp_path, shard_path(shard_index))
                writer = None
                shard_index += 1
                bytes_in_shard = 0

        open_writer()
        if not parts:
            writer.write_table(pa.Table.from_pylist([], schema=schema))
        for part in parts:
            pf = pq.ParquetFile(part)
            for rg in range(pf.num_row_groups):
                table = pf.read_row_group(rg)
                if not table.num_rows:
                    continue
                if writer is None:
                    open_writer()
                writer.write_table(table)
                n_rows += table.num_rows
                distinct.update(table[distinct_column].to_pylist())
                bytes_in_shard += table.nbytes
                if bytes_in_shard >= split_bytes:
                    close_writer()
        close_writer()
        return n_rows, len(distinct)

    # -- reading ------------------------------------------------------------

    @staticmethod
    def iter_batches(
        path: Path, columns: list[str] | None = None, batch_size: int = 10_000
    ) -> Iterator[pa.RecordBatch]:
        yield from pq.ParquetFile(path).iter_batches(
            batch_size=batch_size, columns=columns
        )

    # -- sidecar files in parts/ ---------------------------------------------

    def read_lines(self, name: str) -> set[str]:
        path = self.parts_dir / name
        if not path.exists():
            return set()
        with path.open(encoding="utf-8") as fh:
            return {line.rstrip("\n") for line in fh if line.strip()}

    def append_lines(self, name: str, lines: Sequence[str]) -> None:
        if not lines:
            return
        with (self.parts_dir / name).open("a", encoding="utf-8") as fh:
            for line in lines:
                fh.write(line + "\n")

    def append_jsonl(self, name: str, objs: Sequence[dict]) -> None:
        self.append_lines(name, [json.dumps(o, ensure_ascii=False) for o in objs])

    def read_jsonl(self, name: str) -> Iterator[dict]:
        path = self.parts_dir / name
        if not path.exists():
            return
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield json.loads(line)


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


class EuropeanaApi:
    """Async client for the Europeana APIs: cached, rate-limited, retrying.

    The transport stack keeps hishel's file cache OUTSIDE pyrate-limiter's
    transport, so a cache hit never touches the limiter and a full re-run
    replays from disk in minutes instead of being throttled at rate_limit. The
    cache never expires; hishel caches only 200/301/308, so retries of error
    responses always reach the network.
    """

    USER_AGENT: ClassVar[str] = "europeana-open-newspapers-build/1.0"

    def __init__(self, settings: Settings, errors: ErrorLog) -> None:
        self._settings = settings
        self._errors = errors
        self._auth = {"x-api-key": settings.api_key or ""}
        limiter = limiter_factory.create_inmemory_limiter(
            rate_per_duration=settings.rate_limit, duration=Duration.SECOND
        )
        cached = hishel.AsyncCacheTransport(
            transport=AsyncRateLimiterTransport(limiter=limiter),
            controller=hishel.Controller(force_cache=True, cacheable_methods=["GET"]),
            storage=hishel.AsyncFileStorage(base_path=settings.cache_dir),
        )
        self._client = httpx.AsyncClient(
            transport=cached,
            timeout=httpx.Timeout(60.0),
            follow_redirects=True,
            headers={"User-Agent": self.USER_AGENT},
        )

    async def __aenter__(self) -> "EuropeanaApi":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._client.aclose()

    # -- core ----------------------------------------------------------------

    async def fetch_json(
        self,
        url: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
        phase: str = "",
        record_id: str = "",
    ) -> dict | None:
        """GET a JSON resource with retries; log and return None on final failure."""
        policy = self._settings.retry

        def log_retry(retry_state: RetryCallState) -> None:
            # A single flaky response is routine; surface real outages only.
            if retry_state.attempt_number < 3:
                return
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            tqdm.write(
                f"{phase or 'http'}: retry {retry_state.attempt_number}/{policy.attempts} "
                f"for {record_id or url}: {type(exc).__name__}: {exc}"
            )

        try:
            return await policy.retrying(before_sleep=log_retry)(
                self._get_json, url, params, headers
            )
        except NonRetryableStatus as exc:
            self._errors.log(phase, record_id, f"{exc} for {url}")
            return None
        except (httpx.TransportError, RetryableStatus, InvalidJson) as exc:
            self._errors.log(
                phase,
                record_id,
                f"failed after {policy.attempts} attempts for {url}: "
                f"{type(exc).__name__}: {exc}",
            )
            return None

    async def _get_json(
        self, url: str, params: dict | None, headers: dict | None
    ) -> dict:
        resp = await self._client.get(url, params=params, headers=headers)
        if resp.status_code in self._settings.retry.retryable_statuses:
            raise RetryableStatus(f"HTTP {resp.status_code}")
        if resp.status_code != 200:
            raise NonRetryableStatus(f"HTTP {resp.status_code}")
        try:
            return resp.json()
        except ValueError as exc:
            raise InvalidJson(str(exc)) from exc

    # -- typed endpoints -------------------------------------------------------

    async def _search(
        self, extra_qf: Sequence[str], *, rows: str | None, cursor: str, record_id: str
    ) -> dict | None:
        """One Fulltext Search request; a 200 with `"success": false` is a failure."""
        params = self._settings.base_search_params()
        params["qf"] += list(extra_qf)
        if rows is not None:
            params["rows"] = rows
        params["cursor"] = cursor
        data = await self.fetch_json(
            self._settings.fulltext_search_api,
            params=params,
            headers=self._auth,
            phase="items",
            record_id=record_id,
        )
        if data is None or data.get("success") is False:
            return None
        return data

    async def count(self, extra_qf: Sequence[str]) -> int | None:
        """totalResults for the base query plus extra qf filters (rows=0)."""
        data = await self._search(
            extra_qf, rows="0", cursor="*", record_id=" ".join(extra_qf) or "corpus"
        )
        return None if data is None else data.get("totalResults")

    async def search_page(self, year: int, cursor: str, record_id: str) -> dict | None:
        """One page of a year's cursor chain.

        Only the year range is appended: qf values on the same field are ORed,
        so pairing it with ISSUED_PRESENT would match the entire corpus.
        """
        return await self._search(
            [self._settings.year_qf(year)], rows=None, cursor=cursor, record_id=record_id
        )

    async def entity(self, etype: str, eid: str, uri: str) -> dict | None:
        return await self.fetch_json(
            f"{self._settings.entity_api}/{etype}/{eid}",
            headers=self._auth,
            phase="entities",
            record_id=uri,
        )

    async def manifest(self, url: str, item_id: str) -> dict | None:
        return await self.fetch_json(
            url, params={"format": "3"}, phase="pages", record_id=item_id
        )

    async def annopage(self, url: str, item_id: str) -> dict | None:
        separator = "&" if "?" in url else "?"
        return await self.fetch_json(
            f"{url}{separator}profile=text", phase="pages", record_id=item_id
        )


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


@dataclass
class JobFailure:
    job: Any
    error: Exception


class WorkerPool:
    """The one concurrency primitive every fan-out runs through.

    N workers drain a queue of up-front jobs. A job that raises is recorded as
    a `JobFailure` and never cancels its siblings -- one year partition hitting
    an API outage must not abandon the other 300+ cursor chains mid-flight. The
    caller decides afterwards what the failures mean.
    """

    def __init__(self, workers: int) -> None:
        self._workers = workers

    async def run(
        self, jobs: Iterable[Any], handler: Callable[[Any], Awaitable[None]]
    ) -> list[JobFailure]:
        queue: asyncio.Queue = asyncio.Queue()
        for job in jobs:
            queue.put_nowait(job)
        failures: list[JobFailure] = []

        async def worker() -> None:
            while True:
                try:
                    job = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await handler(job)
                except Exception as exc:  # captured; reported by the caller
                    failures.append(JobFailure(job, exc))

        async with asyncio.TaskGroup() as tg:
            for _ in range(max(1, min(self._workers, queue.qsize()))):
                tg.create_task(worker())
        return failures


# ---------------------------------------------------------------------------
# Parsing (pure, except where an ErrorLog is injected)
# ---------------------------------------------------------------------------


class TitleParser:
    """Title logic shared by the harvest and the sampler.

    Pure (no logging, no settings), so `Sampler` stays importable and runnable
    offline without any pipeline wiring.
    """

    # Every dc_title in the corpus ends in the issue date ("Lienzer Zeitung -
    # 1941-11-15"), in all 11 datasets, for all 995k items. That is the only
    # source of the *exact* date: proxy_dcterms_issued can be filtered but
    # never read (see year_issued).
    DATE_RE: ClassVar[re.Pattern] = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
    # Issue numbering that some datasets append ("..., nr: 16", "... no. 4").
    ISSUE_RE: ClassVar[re.Pattern] = re.compile(
        r"[,;]?\s*(nr|no|n°|nº|num|issue)\b\.?:?\s*\d+", re.IGNORECASE
    )

    @classmethod
    def newspaper_title(cls, dc_title: str | None) -> str:
        """The newspaper's name, with the issue-specific tail stripped off.

        Titles carry the issue date and often an issue number
        ("Hufvudstadsbladet, nr: 16 - 1900-08-09"); both have to go, or every
        issue looks like its own newspaper and the sampler's title round-robin
        does nothing.
        """
        if not dc_title:
            return ""
        text = " ".join(v for values in json.loads(dc_title).values() for v in values)
        text = cls.DATE_RE.sub("", text)
        text = cls.ISSUE_RE.sub("", text)
        return text.strip(" -–,;:") or ""


@dataclass(frozen=True)
class EntityRef:
    """A data.europeana.eu entity URI, split into what the Entity API wants."""

    URI_RE: ClassVar[re.Pattern] = re.compile(
        r"^http://data\.europeana\.eu/(concept|agent|place|timespan)/(?:base/)?(\d+)$"
    )
    TYPE_TO_CLASS: ClassVar[dict[str, str]] = {
        "concept": "skos_Concept",
        "agent": "edm_Agent",
        "place": "edm_Place",
        "timespan": "edm_TimeSpan",
    }

    uri: str
    etype: str
    eid: str

    @classmethod
    def parse(cls, uri: str) -> "EntityRef | None":
        m = cls.URI_RE.match(uri)
        return cls(uri, m.group(1), m.group(2)) if m else None

    @property
    def entity_class(self) -> str:
        return self.TYPE_TO_CLASS[self.etype]

    @classmethod
    def class_for(cls, uri: str, fallback: str) -> str:
        ref = cls.parse(uri)
        return ref.entity_class if ref else fallback


class ItemExtractor:
    """One Search API item -> (item row, enrichment edges, label hints)."""

    def __init__(self, settings: Settings, errors: ErrorLog) -> None:
        self._settings = settings
        self._errors = errors

    def is_newspaper_item(self, item: dict) -> bool:
        name = self._first(item.get("edmDatasetName", []))
        return (
            bool(name)
            and self._settings.newspaper_dataset_substring in name.lower()
        )

    def extract(self, item: dict, year: int) -> tuple[dict, list[dict], list[dict]]:
        """`year` is the publication year of the partition the item came from."""
        iid = item["id"]
        item_uri = self._settings.item_uri_prefix + iid
        titles = item.get("dcTitleLangAware", {})

        row = {
            "item_id": item_uri,
            "dc_title": json.dumps(titles, ensure_ascii=False),
            "dc_description": self._dumps(item.get("dcDescriptionLangAware")),
            "dc_type": self._dumps(item.get("dcTypeLangAware")),
            "dc_type_en": None,
            "dc_subject_en": None,
            "dc_creator_en": None,
            "enriched_concepts": None,
            "enriched_agents": None,
            "enriched_places": None,
            "enriched_timespans": None,
            "language": self._first(item.get("language", [])),
            "country": self._first(item.get("country", [])),
            "year_issued": year,
            "date_issued": self._date_from_title(titles, year, item_uri),
            "data_provider": self._first(item.get("dataProvider", [])),
            "provider": self._first(item.get("provider", [])),
            "dataset_name": self._first(item.get("edmDatasetName", [])),
            "manifest_url": f"{self._settings.iiif_presentation}{iid}/manifest",
            "europeana_url": f"https://www.europeana.eu/item{iid}",
            "image_rights": self._first(item.get("rights", [])),
            "theme": "newspaper",  # the query is restricted to theme=newspaper
        }

        def def_uris(key: str) -> list[str]:
            values = (item.get(key) or {}).get("def") or []
            return [v for v in values if isinstance(v, str) and v.startswith("http://")]

        edges: dict[str, tuple[str, str]] = {}  # uri -> (class, source property)
        for key, source, fallback in (
            ("dcTypeLangAware", "dc_type", "skos_Concept"),
            ("dcSubjectLangAware", "dc_subject", "skos_Concept"),
            ("dcCreatorLangAware", "dc_creator", "edm_Agent"),
        ):
            for uri in def_uris(key):
                edges.setdefault(uri, (EntityRef.class_for(uri, fallback), source))
        for key, source, fallback in (
            ("edmConcept", "edm_concept", "skos_Concept"),
            ("edmAgent", "edm_agent", "edm_Agent"),
            ("edmPlace", "dcterms_spatial", "edm_Place"),
            ("edmTimespan", "dcterms_temporal", "edm_TimeSpan"),
        ):
            for uri in item.get(key) or []:
                if isinstance(uri, str) and uri.startswith("http://"):
                    edges.setdefault(uri, (EntityRef.class_for(uri, fallback), source))

        edge_rows = [
            {
                "item_id": item_uri,
                "entity_uri": uri,
                "entity_class": cls,
                "source_property": src,
            }
            for uri, (cls, src) in edges.items()
        ]

        # Third-party entity URIs are not resolvable via the Entity API.
        # Capture an English label only when the mapping is unambiguous: the
        # item has exactly one entity URI of that class (the third-party one)
        # and exactly one English label in the corresponding label field.
        hints: list[dict] = []
        for cls, label_field in (
            ("skos_Concept", "edmConceptPrefLabelLangAware"),
            ("edm_Agent", "edmAgentLabelLangAware"),
        ):
            class_uris = [u for u, (c, _) in edges.items() if c == cls]
            third_party = [
                u
                for u in class_uris
                if not u.startswith(self._settings.data_europeana_prefix)
            ]
            if len(class_uris) == 1 and len(third_party) == 1:
                en_labels = (item.get(label_field) or {}).get("en") or []
                if len(en_labels) == 1:
                    hints.append({"uri": third_party[0], "label": en_labels[0]})
        return row, edge_rows, hints

    def _date_from_title(self, titles: dict, year: int, item_uri: str) -> date | None:
        """Exact issue date out of the dc_title labels.

        The title is a display label while the year came from the
        dcterms:issued index, so the two can disagree (one item in ~995k does,
        across a New Year boundary). Both are kept as harvested; the
        disagreement is logged rather than quietly reconciled.
        """
        text = " ".join(v for values in titles.values() for v in values)
        match = TitleParser.DATE_RE.search(text)
        if not match:
            self._errors.log("items", item_uri, f"no date in title: {text[:80]!r}")
            return None
        try:
            issued = date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            self._errors.log(
                "items", item_uri, f"invalid date in title: {match.group(0)}"
            )
            return None
        if issued.year != year:
            self._errors.log(
                "items",
                item_uri,
                f"title date {issued} disagrees with dcterms:issued year {year}",
            )
        return issued

    @staticmethod
    def _first(values: list | None) -> Any:
        return values[0] if values else None

    @staticmethod
    def _dumps(value: Any) -> str | None:
        return json.dumps(value, ensure_ascii=False) if value else None


class EntityParser:
    """Entity API JSON -> rows for entities.parquet. Pure.

    The API mixes plain and prefixed keys ("broader" vs "skos:broader")
    depending on the entity, so every field is looked up under all spellings.
    """

    LABELS: ClassVar[tuple[str, ...]] = ("prefLabel", "altLabel")
    FIELDS: ClassVar[tuple[tuple[str, tuple[str, ...]], ...]] = (
        ("broader", ("broader", "skos:broader")),
        ("narrower", ("narrower", "skos:narrower")),
        ("sameAs", ("sameAs", "owl:sameAs")),
        ("exactMatch", ("exactMatch", "skos:exactMatch")),
        ("lat", ("lat", "latitude", "wgs84_pos:lat")),
        ("long", ("long", "longitude", "wgs84_pos:long")),
        ("begin", ("begin", "edm:begin")),
        ("end", ("end", "edm:end")),
        ("dateOfBirth", ("dateOfBirth", "rdaGr2:dateOfBirth")),
        ("dateOfDeath", ("dateOfDeath", "rdaGr2:dateOfDeath")),
    )

    @classmethod
    def rows(cls, uri: str, entity_class: str, data: dict) -> list[dict]:
        rows: list[dict] = []

        def add(field_name: str, value: Any, language: str | None = None) -> None:
            if value is not None:
                rows.append(
                    {
                        "entity_uri": uri,
                        "entity_class": entity_class,
                        "field": field_name,
                        "value": str(value),
                        "language": language,
                    }
                )

        for field_name in cls.LABELS:
            labels = cls._get(data, field_name, f"skos:{field_name}") or {}
            if isinstance(labels, dict):
                for lang, values in labels.items():
                    for value in cls._as_list(values):
                        add(field_name, value, lang)

        for field_name, keys in cls.FIELDS:
            for value in cls._as_list(cls._get(data, *keys)):
                if isinstance(value, dict):
                    value = value.get("id") or value.get("@id")
                add(field_name, value)
        return rows

    @staticmethod
    def _as_list(value: Any) -> list:
        if value is None:
            return []
        return value if isinstance(value, list) else [value]

    @staticmethod
    def _get(data: dict, *keys: str) -> Any:
        for key in keys:
            if data.get(key) is not None:
                return data[key]
        return None


class PageParser:
    """IIIF annotation-page JSON -> page text, rights and word annotations."""

    CHAR_RE: ClassVar[re.Pattern] = re.compile(r"#char=(\d+),(\d+)")
    XYWH_RE: ClassVar[re.Pattern] = re.compile(r"#xywh=(\d+),(\d+),(\d+),(\d+)")

    def __init__(self, errors: ErrorLog) -> None:
        self._errors = errors

    def parse(self, data: dict, url: str, item_id: str) -> dict | None:
        resources = data.get("resources") or []
        page_ann = next(
            (r for r in resources if r.get("textGranularity") == "page"), None
        )
        if page_ann is None:
            self._errors.log("pages", item_id, f"no page-level annotation in {url}")
            return None
        resource = page_ann.get("resource") or {}
        text = resource.get("value")
        if text is None:
            self._errors.log(
                "pages", item_id, f"page-level annotation without text value in {url}"
            )
            return None

        annotations = []
        for res in resources:
            granularity = res.get("textGranularity")
            if granularity not in ("block", "line", "word"):
                continue
            resource_id = (res.get("resource") or {}).get("@id") or ""
            char_match = self.CHAR_RE.search(resource_id)
            on = res.get("on")
            on0 = on[0] if isinstance(on, list) and on else (on if isinstance(on, str) else "")
            bbox_match = self.XYWH_RE.search(on0 or "")
            char_start, char_end = (
                (int(char_match.group(1)), int(char_match.group(2)))
                if char_match
                else (None, None)
            )
            annotations.append(
                {
                    "granularity": granularity,
                    "text": text[char_start:char_end] if char_match else None,
                    "char_start": char_start,
                    "char_end": char_end,
                    "bbox_x": int(bbox_match.group(1)) if bbox_match else None,
                    "bbox_y": int(bbox_match.group(2)) if bbox_match else None,
                    "bbox_w": int(bbox_match.group(3)) if bbox_match else None,
                    "bbox_h": int(bbox_match.group(4)) if bbox_match else None,
                }
            )
        return {
            "text": text,
            "language": data.get("language") or resource.get("language"),
            "text_rights": resource.get("edmRights") or data.get("edmRights"),
            "annotations_json": json.dumps(annotations, ensure_ascii=False),
            "page_id": self.annopage_id(url),
        }

    @staticmethod
    def annopage_id(url: str) -> str | None:
        segments = urlsplit(url).path.rstrip("/").split("/")
        if "annopage" in segments:
            idx = segments.index("annopage")
            return "/".join(segments[idx + 1 :]) or None
        return segments[-1] or None


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


class Sampler:
    """Pick `size` issues in total, stratified all the way down.

    strategy:
      proportional -- each dataset's share mirrors its share of the corpus, so
                      the sample is a miniature of the real thing (a third of
                      it Dutch).
      balanced     -- every dataset gets an equal share, so small collections
                      are as visible as large ones. Diverse, not representative.

    Under either strategy the quota is then split the same way, because the
    corpus is lopsided in three different directions at once:
      * decade -- proportional within the dataset, floor of 1, so no period
                  vanishes
      * title  -- round-robin, so one newspaper cannot eat a dataset's quota
      * issue  -- ordered by a hash of the item id, not lexicographically

    Datasets are keyed on dataset_name, not data_provider: data_provider is a
    free-text label with spelling variants (the Austrian National Library
    appears under two names for a single dataset, and would draw a double
    quota).

    That last stratum matters more than it looks: item ids sort by title and
    then by date, so taking the first N gave 25 consecutive issues of one
    newspaper. The hash is md5, not hash(), which is salted per process and
    would make runs unreproducible.

    Pure and deterministic: reads only items.parquet, no network -- import it
    and inspect the distribution to verify sampling changes against the real
    corpus.
    """

    def __init__(self, size: int, strategy: SampleStrategy) -> None:
        self._size = size
        self._strategy = strategy

    def build(self, items_path: Path) -> list[dict]:
        # dataset -> decade -> title -> [(sort_key, item_id, manifest_url)]
        groups: dict[str, dict[int, dict[str, list]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        for batch in ParquetStore.iter_batches(
            items_path,
            columns=["item_id", "dataset_name", "dc_title", "year_issued", "manifest_url"],
        ):
            for iid, dataset, dc_title, year, manifest_url in zip(
                batch["item_id"].to_pylist(),
                batch["dataset_name"].to_pylist(),
                batch["dc_title"].to_pylist(),
                batch["year_issued"].to_pylist(),
                batch["manifest_url"].to_pylist(),
            ):
                decade = (year // 10 * 10) if year is not None else 0
                key = hashlib.md5(iid.encode("utf-8")).hexdigest()
                groups[dataset or ""][decade][TitleParser.newspaper_title(dc_title)].append(
                    (key, iid, manifest_url)
                )

        dataset_size = {
            ds: sum(len(v) for titles in decades.values() for v in titles.values())
            for ds, decades in groups.items()
        }
        if self._strategy is SampleStrategy.BALANCED:
            per_dataset = self.allocate_equal(self._size, dataset_size)
        else:
            per_dataset = self.allocate(self._size, dataset_size)

        sample: list[dict] = []
        for dataset in sorted(groups):
            decades = groups[dataset]
            per_decade = self.allocate(
                per_dataset.get(dataset, 0),
                {d: sum(len(v) for v in titles.values()) for d, titles in decades.items()},
            )
            for decade in sorted(per_decade):
                titles = decades[decade]
                for issues in titles.values():
                    issues.sort()  # by md5 -> deterministic, not by title/date
                order = sorted(titles, key=lambda t: (-len(titles[t]), t))
                pointers = dict.fromkeys(order, 0)
                picked = 0
                while picked < per_decade[decade]:
                    progressed = False
                    for title in order:  # round-robin across newspapers
                        pos = pointers[title]
                        if pos < len(titles[title]):
                            _, iid, manifest_url = titles[title][pos]
                            sample.append({"item_id": iid, "manifest_url": manifest_url})
                            pointers[title] = pos + 1
                            picked += 1
                            progressed = True
                            if picked >= per_decade[decade]:
                                break
                    if not progressed:
                        break
        return sample

    @staticmethod
    def allocate(quota: int, sizes: dict) -> dict:
        """Split `quota` across strata proportionally to `sizes`, never starving one.

        Every non-empty stratum gets at least one item (up to what it holds);
        the rest is handed out by largest remainder. Without the floor, a
        proportional split silently drops whole decades -- the 1820s are 3.7%
        of the corpus and rounded to zero before.
        """
        strata = {k: v for k, v in sizes.items() if v}
        if not strata:
            return {}
        alloc = {k: 1 for k in strata}  # the floor
        if sum(alloc.values()) > quota:  # more strata than quota: keep the biggest
            keep = sorted(strata, key=lambda k: (-strata[k], k))[:quota]
            return {k: 1 for k in keep}

        total = sum(strata.values())
        remaining = quota - len(alloc)
        shares = {k: remaining * n / total for k, n in strata.items()}
        for k in shares:
            alloc[k] += int(shares[k])
        # largest remainder for the leftovers
        leftover = quota - sum(alloc.values())
        for k in sorted(shares, key=lambda k: (-(shares[k] % 1), k))[:leftover]:
            alloc[k] += 1
        # never allocate more than a stratum actually holds
        for k in alloc:
            alloc[k] = min(alloc[k], strata[k])
        return alloc

    @staticmethod
    def allocate_equal(quota: int, capacities: dict) -> dict:
        """Split `quota` as evenly as possible within each stratum's capacity.

        Whatever a small stratum cannot absorb is redistributed among the
        others, so a balanced sample of 1000 still yields 1000 items even
        though Luxembourg only has 1,317 to give.
        """
        alloc = dict.fromkeys(capacities, 0)
        open_strata = {k for k, v in capacities.items() if v}
        remaining = quota
        while remaining and open_strata:
            share, extra = divmod(remaining, len(open_strata))
            if not share:  # fewer left than strata: one by one, biggest first
                for k in sorted(open_strata, key=lambda k: (-capacities[k], k))[:extra]:
                    alloc[k] += 1
                break
            progressed = False
            for k in sorted(open_strata):
                room = capacities[k] - alloc[k]
                take = min(share, room)
                alloc[k] += take
                remaining -= take
                if take:
                    progressed = True
                if alloc[k] >= capacities[k]:
                    open_strata.discard(k)
            if not progressed:
                break
        return {k: v for k, v in alloc.items() if v}


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------


class Phase(ABC):
    """One pipeline phase: shared wiring plus the worker/queue plumbing.

    `run()` is a template: skip if finalized, otherwise execute and report the
    elapsed time -- so every phase greets, resumes and signs off the same way.
    """

    name: ClassVar[str]

    def __init__(
        self,
        settings: Settings,
        api: EuropeanaApi,
        checkpoint: Checkpoint,
        store: ParquetStore,
        errors: ErrorLog,
        metadata: MetadataFile,
    ) -> None:
        self._settings = settings
        self._api = api
        self._checkpoint = checkpoint
        self._store = store
        self._errors = errors
        self._metadata = metadata

    async def run(self) -> None:
        if self._finalized:
            self._echo("already finalized, skipping (delete checkpoint.json to redo)")
            return
        started = perf_counter()
        await self._execute()
        self._echo(f"done in {Fmt.duration(perf_counter() - started)}")

    @property
    @abstractmethod
    def _finalized(self) -> bool: ...

    @abstractmethod
    async def _execute(self) -> None: ...

    def _echo(self, message: str) -> None:
        click.echo(f"{self.name}: {message}")

    async def _run_pipeline(
        self,
        jobs: Iterable[Any],
        handler: Callable[[Any, Callable[[Any], Awaitable[None]]], Awaitable[None]],
        write: Callable[[Any], Awaitable[None]],
        finish: Callable[[], Awaitable[None]] | None = None,
    ) -> list[JobFailure]:
        """jobs -> WorkerPool -> bounded results queue -> one writer coroutine.

        Handlers emit result messages through their `emit` argument; the writer
        owns ALL disk I/O (part files, done-lists, checkpoint saves), so no two
        writes or checkpoint saves can ever interleave. The pool captures
        per-job exceptions -- one failed job must not cancel the rest -- while
        a writer exception cancels everything via the TaskGroup: if the disk is
        broken, nothing is worth continuing.
        """
        results: asyncio.Queue = asyncio.Queue(maxsize=8)  # backpressure

        async def drain() -> None:
            while (msg := await results.get()) is not None:
                await write(msg)
            if finish is not None:
                await finish()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(drain())
            failures = await WorkerPool(self._settings.concurrency).run(
                jobs, lambda job: handler(job, results.put)
            )
            await results.put(None)  # sentinel: all workers are done
        return failures

    def _log_failures(self, failures: list[JobFailure]) -> None:
        for failure in failures:
            self._errors.log(
                self.name,
                str(failure.job),
                f"{type(failure.error).__name__}: {failure.error}",
            )


class PartitionFailed(Exception):
    """A year's cursor chain gave up after retries; it must resume in a later run."""


@dataclass
class ItemsFlush:
    """Everything one cursor chain hands the writer at a flush point.

    The worker snapshots `cursor` and `requests` together when it builds the
    flush, and the writer persists them in one checkpoint save -- they describe
    the same point in the chain and must never diverge on disk.
    """

    year: str
    items: list[dict]
    enrichments: list[dict]
    hints: list[dict]
    cursor: str | None
    requests: int
    done: bool
    skipped: int  # non-newspaper items dropped since the previous flush


class ItemsPhase(Phase):
    """Harvest every issue's metadata: one cursor chain per publication year.

    Cursor pagination is inherently serial (each request needs the previous
    response's nextCursor), so a single chain is capped at ~1.3 req/s no matter
    what --rate-limit says. Running one chain per year concurrently makes the
    rate limit the binding constraint -- and the partition is what gives each
    item its year_issued, which the API otherwise refuses to hand over.
    """

    name = "items"

    _pbar: tqdm | None = None
    _issued = 0  # requests issued this run, shown as a bar postfix

    @property
    def _state(self) -> ItemsState:
        return self._checkpoint.items

    @property
    def _finalized(self) -> bool:
        return self._state.finalized

    @cached_property
    def _extractor(self) -> ItemExtractor:
        return ItemExtractor(self._settings, self._errors)

    async def _execute(self) -> None:
        if not self._state.done:
            await self._harvest()
        self._merge_outputs()
        self._checkpoint.save()

    # -- harvesting -----------------------------------------------------------

    async def _harvest(self) -> None:
        st = self._state
        if not st.partitions:
            st.partitions = await self._discover_partitions()
            self._checkpoint.save()

        years = sorted(st.partitions, key=int)
        if self._settings.max_partitions:
            years = years[: self._settings.max_partitions]
            self._echo(f"limited to {len(years)} year partitions (--max-partitions)")

        # Progress counts completed partitions, not requests: a resumed chain
        # replays every request since its last flushed cursor (from cache), so
        # a per-request counter double-counts on every resume and eventually
        # creeps past its total -- at which point tqdm silently drops the bar.
        # Partitions only ever complete once.
        todo = [y for y in years if not st.partitions[y].done]
        if len(todo) < len(years):
            self._echo(f"resuming: {len(years) - len(todo)}/{len(years)} year partitions already done")
        self._issued = 0
        self._pbar = tqdm(
            desc="items: year partitions",
            unit="year",
            total=len(years),
            initial=len(years) - len(todo),
        )
        try:
            failures = await self._run_pipeline(todo, self._harvest_year, self._write_flush)
        finally:
            self._pbar.close()
        self._log_failures(failures)

        st.done = all(st.partitions[y].done for y in years)
        self._checkpoint.save()
        # Finalizing marks the phase complete and makes every future run skip
        # it. Doing that with years still missing would silently ship a dataset
        # with entire decades absent -- with no error anywhere.
        if not st.done:
            incomplete = [y for y in years if not st.partitions[y].done]
            raise click.ClickException(
                f"items: {len(incomplete)} of {len(years)} year partitions incomplete "
                f"({', '.join(incomplete[:10])}{' ...' if len(incomplete) > 10 else ''}). "
                f"Nothing was finalized; re-run the same command to resume."
            )

    async def _discover_partitions(self) -> dict[str, PartitionState]:
        """Count items per year and verify the years cover the whole corpus.

        A filter that silently matches everything (or nothing) would otherwise
        produce a plausible-looking but wrong dataset, so the sum is checked
        against totalResults before a single item is harvested.
        """
        # Counted with ISSUED_PRESENT (not the bare query): the year partitions
        # can only ever cover date-bearing items, so that is the total they
        # must reconcile against.
        total = await self._api.count([Settings.ISSUED_PRESENT])
        if total is None:
            raise click.ClickException("items: could not count the corpus; check the API key")

        years = list(range(self._settings.year_min, self._settings.year_max))
        counts: dict[int, int] = {}
        pbar = tqdm(desc="items: counting years", unit="year", total=len(years))

        async def count_year(year: int) -> None:
            count = await self._api.count([Settings.year_qf(year)])
            pbar.update(1)
            if count is None:
                raise ValueError(f"count failed for {year}")
            counts[year] = count

        try:
            failures = await WorkerPool(self._settings.concurrency).run(years, count_year)
        finally:
            pbar.close()
        if failures:
            raise click.ClickException("items: some year counts failed; re-run to retry")

        covered = sum(counts.values())
        if covered != total:
            raise click.ClickException(
                f"items: year partitions cover {Fmt.count(covered)} items but the "
                f"corpus has {Fmt.count(total)}. Widen year_min/year_max "
                f"({self._settings.year_min}-{self._settings.year_max}) to cover "
                f"every publication year."
            )
        partitions = {
            str(year): PartitionState(count=count)
            for year, count in sorted(counts.items())
            if count
        }
        self._echo(
            f"{Fmt.count(total)} items across {len(partitions)} year partitions "
            f"({min(partitions, key=int)}-{max(partitions, key=int)}), counts verified"
        )
        return partitions

    async def _harvest_year(
        self, year: str, emit: Callable[[ItemsFlush], Awaitable[None]]
    ) -> None:
        """Drive one year's serial cursor chain.

        The chain terminates only when the API returns no nextCursor -- which
        it does one request *after* the last populated page, so a chain of N
        items takes ceil(N/100)+1 requests.
        """
        pstate = self._state.partitions[year]
        cursor = pstate.cursor
        requests = pstate.requests
        items: list[dict] = []
        enrichments: list[dict] = []
        hints: list[dict] = []
        skipped = 0
        since_flush = 0

        async def flush(next_cursor: str | None, done: bool) -> None:
            nonlocal items, enrichments, hints, skipped
            await emit(
                ItemsFlush(year, items, enrichments, hints, next_cursor, requests, done, skipped)
            )
            # fresh lists: the writer owns the emitted ones now
            items, enrichments, hints, skipped = [], [], [], 0

        while cursor:
            data = await self._api.search_page(
                int(year), cursor, record_id=f"year {year} request #{requests}"
            )
            if data is None:
                # A cursor chain cannot skip a page; save progress and give up
                # on this partition (the other chains keep running).
                await flush(cursor, done=False)
                raise PartitionFailed(f"year {year} failed after retries; re-run to resume")
            for item in data.get("items") or []:
                if not self._extractor.is_newspaper_item(item):
                    skipped += 1
                    continue
                try:
                    row, edges, item_hints = self._extractor.extract(item, int(year))
                except Exception as exc:
                    self._errors.log(
                        "items", str(item.get("id")), f"{type(exc).__name__}: {exc}"
                    )
                    continue
                items.append(row)
                enrichments.extend(edges)
                hints.extend(item_hints)

            requests += 1
            since_flush += 1
            self._issued += 1
            self._pbar.set_postfix_str(f"{self._issued} requests", refresh=False)
            cursor = data.get("nextCursor")
            if self._settings.max_requests and requests >= self._settings.max_requests:
                tqdm.write(
                    f"items: year {year} stopping early at "
                    f"--max-requests={self._settings.max_requests}"
                )
                cursor = None
            if since_flush >= self._settings.items_flush_every:
                await flush(cursor, done=False)
                since_flush = 0
        await flush(None, done=True)

    async def _write_flush(self, msg: ItemsFlush) -> None:
        """The only place items state touches disk.

        Part numbering, the skipped counter and the cursor live here, so no two
        partitions' checkpoint saves can interleave -- and cursor and requests
        always land in one atomic save.
        """
        pstate = self._state.partitions[msg.year]
        if msg.items:
            stem = f"{msg.year}_{pstate.part:05d}"
            await asyncio.to_thread(
                self._store.write_part, f"items_{stem}.parquet", msg.items, Schemas.ITEMS
            )
            await asyncio.to_thread(
                self._store.write_part,
                f"enrich_{stem}.parquet",
                msg.enrichments,
                Schemas.ENRICHMENTS,
            )
            pstate.part += 1
        if msg.hints:
            self._store.append_jsonl("thirdparty_labels.jsonl", msg.hints)
        pstate.cursor = msg.cursor
        pstate.requests = msg.requests
        pstate.done = msg.done
        self._state.skipped_non_newspaper += msg.skipped
        self._checkpoint.save()
        if msg.done:
            self._pbar.update(1)

    # -- merging ---------------------------------------------------------------

    def _merge_outputs(self) -> None:
        """Merge parts into items.parquet and enrichments.parquet, deduplicated.

        Duplicates exist only where a crash replayed an unflushed window.
        Deduplication is a boolean mask over the item_id column, so entire
        Arrow tables pass through without a per-row Python round-trip.
        """
        item_parts = self._store.parts("items_*.parquet")
        seen: set[str] = set()
        dup_ids: set[str] = set()
        institutions: set[str] = set()
        datasets: set[str] = set()
        n_items = 0
        with self._store.open_output("items.parquet", Schemas.ITEMS) as writer:
            if not item_parts:
                writer.write_table(pa.Table.from_pylist([], schema=Schemas.ITEMS))
            for part in tqdm(item_parts, desc="items: merging parts", unit="part"):
                table = pq.read_table(part)
                mask = []
                for iid in table["item_id"].to_pylist():
                    fresh = iid not in seen
                    if fresh:
                        seen.add(iid)
                    else:
                        dup_ids.add(iid)
                    mask.append(fresh)
                if not all(mask):
                    table = table.filter(pa.array(mask))
                if table.num_rows:
                    institutions.update(pc.unique(table["data_provider"]).drop_null().to_pylist())
                    datasets.update(pc.unique(table["dataset_name"]).drop_null().to_pylist())
                    writer.write_table(table)
                    n_items += table.num_rows

        n_edges = self._merge_enrichments(dup_ids)

        self._state.finalized = True
        skipped = self._state.skipped_non_newspaper
        self._metadata.update(
            "items",
            {
                "items": n_items,
                "institutions": len(institutions),
                "datasets": len(datasets),
                "enrichment_edges": n_edges,
                "skipped_non_newspaper_datasets": skipped,
            },
        )
        self._echo(
            f"wrote {Fmt.count(n_items)} items ({len(institutions)} institutions, "
            f"{len(datasets)} datasets, {Fmt.count(n_edges)} enrichment edges; "
            f"{skipped} skipped as non-newspaper datasets)"
        )

    def _merge_enrichments(self, dup_ids: set[str]) -> int:
        """Merge edge parts; deduplicate only edges of items that were seen twice."""
        enrich_parts = self._store.parts("enrich_*.parquet")
        dup_array = pa.array(sorted(dup_ids)) if dup_ids else None
        dup_edges_seen: set[tuple] = set()
        n_edges = 0
        with self._store.open_output("enrichments.parquet", Schemas.ENRICHMENTS) as writer:
            if not enrich_parts:
                writer.write_table(pa.Table.from_pylist([], schema=Schemas.ENRICHMENTS))
            for part in enrich_parts:
                table = pq.read_table(part)
                if not table.num_rows:
                    continue
                has_dups = dup_array is not None and pc.any(
                    pc.is_in(table["item_id"], value_set=dup_array)
                ).as_py()
                if not has_dups:  # the common case: pass the table straight through
                    writer.write_table(table)
                    n_edges += table.num_rows
                    continue
                keep = []
                for row in table.to_pylist():
                    if row["item_id"] in dup_ids:
                        key = (row["item_id"], row["entity_uri"], row["source_property"])
                        if key in dup_edges_seen:
                            continue
                        dup_edges_seen.add(key)
                    keep.append(row)
                if keep:
                    writer.write_table(pa.Table.from_pylist(keep, schema=Schemas.ENRICHMENTS))
                    n_edges += len(keep)
        return n_edges


@dataclass
class EntityResult:
    uri: str
    rows: list[dict] | None  # None: fetch failed (already logged); still done


class EntitiesPhase(Phase):
    """Resolve the entity URIs the items link to, then fill the convenience
    columns on items.parquet from the resolved labels and facts."""

    name = "entities"
    DONE_LIST: ClassVar[str] = "entities_done.txt"

    _pbar: tqdm | None = None

    @property
    def _state(self) -> EntitiesState:
        return self._checkpoint.entities

    @property
    def _finalized(self) -> bool:
        return self._state.finalized and self._state.post_done

    async def _execute(self) -> None:
        enrichments_path = self._store.output_dir / "enrichments.parquet"
        items_path = self._store.output_dir / "items.parquet"
        if not enrichments_path.exists() or not items_path.exists():
            raise click.ClickException("entities: run the items phase first")
        if not self._state.finalized:
            await self._resolve(enrichments_path)
        if not self._state.post_done:
            self._postprocess_items(items_path)
            self._state.post_done = True
            self._checkpoint.save()

    # -- resolving --------------------------------------------------------------

    async def _resolve(self, enrichments_path: Path) -> None:
        refs = self._resolvable_refs(enrichments_path)
        done = self._store.read_lines(self.DONE_LIST)
        todo = [ref for ref in refs if ref.uri not in done]
        if len(todo) < len(refs):
            self._echo(f"resuming: {len(refs) - len(todo)}/{len(refs)} entities already resolved")
        self._rows: list[dict] = []
        self._done_uris: list[str] = []
        self._pbar = tqdm(
            desc="entities", unit="entity", total=len(refs), initial=len(refs) - len(todo)
        )
        try:
            failures = await self._run_pipeline(
                todo, self._fetch_entity, self._write_result, self._flush
            )
        finally:
            self._pbar.close()
        self._log_failures(failures)
        if failures:
            raise click.ClickException(
                f"entities: {len(failures)} entities hit unexpected errors; re-run to resume"
            )

        n_facts = self._store.merge("entities_*.parquet", "entities.parquet", Schemas.ENTITIES)
        self._state.finalized = True
        self._checkpoint.save()
        self._metadata.update(
            "entities", {"resolvable_entities": len(refs), "entity_facts": n_facts}
        )
        self._echo(f"resolved {len(refs)} entities ({Fmt.count(n_facts)} facts)")

    def _resolvable_refs(self, enrichments_path: Path) -> list[EntityRef]:
        uris = sorted(
            pc.unique(
                pq.read_table(enrichments_path, columns=["entity_uri"])["entity_uri"]
            ).to_pylist()
        )
        refs: list[EntityRef] = []
        for uri in uris:
            if not uri.startswith(self._settings.data_europeana_prefix):
                continue  # third-party URI: not resolvable via the Entity API
            ref = EntityRef.parse(uri)
            if ref is None:
                self._errors.log("entities", uri, "unparseable Europeana entity URI")
                continue
            refs.append(ref)
        return refs

    async def _fetch_entity(
        self, ref: EntityRef, emit: Callable[[EntityResult], Awaitable[None]]
    ) -> None:
        data = await self._api.entity(ref.etype, ref.eid, ref.uri)
        self._pbar.update(1)
        rows = EntityParser.rows(ref.uri, ref.entity_class, data) if data is not None else None
        await emit(EntityResult(ref.uri, rows))

    async def _write_result(self, msg: EntityResult) -> None:
        self._done_uris.append(msg.uri)
        if msg.rows:
            self._rows.extend(msg.rows)
        if len(self._done_uris) >= self._settings.entity_batch:
            await self._flush()

    async def _flush(self) -> None:
        if self._rows:
            name = f"entities_{self._state.part:05d}.parquet"
            await asyncio.to_thread(self._store.write_part, name, self._rows, Schemas.ENTITIES)
            self._state.part += 1
            self._rows = []
        if self._done_uris:
            self._store.append_lines(self.DONE_LIST, self._done_uris)
            self._done_uris = []
        self._checkpoint.save()

    # -- postprocessing ----------------------------------------------------------

    def _postprocess_items(self, items_path: Path) -> None:
        """Fill dc_*_en and enriched_* columns in items.parquet from entities.

        Only the seven computed columns are built in Python; the other fifteen
        pass through as Arrow arrays via set_column, untouched.
        """
        label_en, facts = self._entity_lookup()
        edges = self._edges_by_item()

        def enriched_json(item_edges: tuple, cls: str, extra: dict[str, str]) -> str | None:
            objs = []
            for uri, edge_cls, src in item_edges:
                if edge_cls != cls:
                    continue
                obj = {"uri": uri, "label_en": label_en.get(uri), "source": src}
                for out_key, fact_key in extra.items():
                    obj[out_key] = facts.get(uri, {}).get(fact_key)
                objs.append(obj)
            return json.dumps(objs, ensure_ascii=False) if objs else None

        def labels_for(item_edges: tuple, source: str) -> list[str]:
            return [label_en[u] for u, _, s in item_edges if s == source and u in label_en]

        n_rows = 0
        with self._store.open_output("items.parquet", Schemas.ITEMS) as writer:
            for batch in tqdm(
                ParquetStore.iter_batches(items_path),
                desc="entities: updating items.parquet",
                unit="batch",
            ):
                columns: dict[str, list] = defaultdict(list)
                for iid in batch["item_id"].to_pylist():
                    item_edges = edges.get(iid, ())
                    columns["dc_type_en"].append(labels_for(item_edges, "dc_type"))
                    columns["dc_subject_en"].append(labels_for(item_edges, "dc_subject"))
                    columns["dc_creator_en"].append(labels_for(item_edges, "dc_creator"))
                    columns["enriched_concepts"].append(enriched_json(item_edges, "skos_Concept", {}))
                    columns["enriched_agents"].append(enriched_json(item_edges, "edm_Agent", {}))
                    columns["enriched_places"].append(
                        enriched_json(item_edges, "edm_Place", {"lat": "lat", "lon": "lon"})
                    )
                    columns["enriched_timespans"].append(
                        enriched_json(item_edges, "edm_TimeSpan", {"begin": "begin", "end": "end"})
                    )
                table = pa.Table.from_batches([batch])
                for name, values in columns.items():
                    field_ = Schemas.ITEMS.field(name)
                    table = table.set_column(
                        table.schema.get_field_index(name),
                        field_,
                        pa.array(values, type=field_.type),
                    )
                writer.write_table(table)
                n_rows += table.num_rows
            if n_rows == 0:
                writer.write_table(pa.Table.from_pylist([], schema=Schemas.ITEMS))
        self._echo(f"updated convenience columns on {Fmt.count(n_rows)} items")

    def _entity_lookup(self) -> tuple[dict[str, str], dict[str, dict]]:
        """English labels and location/time facts, keyed by entity URI."""
        label_en: dict[str, str] = {}
        facts: dict[str, dict] = defaultdict(dict)
        entities_path = self._store.output_dir / "entities.parquet"
        if entities_path.exists():
            for batch in ParquetStore.iter_batches(entities_path):
                for uri, field_name, value, lang in zip(
                    batch["entity_uri"].to_pylist(),
                    batch["field"].to_pylist(),
                    batch["value"].to_pylist(),
                    batch["language"].to_pylist(),
                ):
                    if field_name == "prefLabel" and lang == "en":
                        label_en.setdefault(uri, value)
                    elif field_name in ("lat", "long", "begin", "end"):
                        facts[uri].setdefault(
                            "lon" if field_name == "long" else field_name, value
                        )
        for hint in self._store.read_jsonl("thirdparty_labels.jsonl"):
            label_en.setdefault(hint["uri"], hint["label"])
        return label_en, facts

    def _edges_by_item(self) -> dict[str, tuple]:
        """All enrichment edges keyed by item; strings interned via a small
        cache so repeated URIs/classes share one object across 995k items."""
        intern: dict[str, str] = {}
        edges: dict[str, list] = defaultdict(list)
        for batch in ParquetStore.iter_batches(self._store.output_dir / "enrichments.parquet"):
            for item_id, uri, cls, src in zip(
                batch["item_id"].to_pylist(),
                batch["entity_uri"].to_pylist(),
                batch["entity_class"].to_pylist(),
                batch["source_property"].to_pylist(),
            ):
                edges[item_id].append(
                    (
                        intern.setdefault(uri, uri),
                        intern.setdefault(cls, cls),
                        intern.setdefault(src, src),
                    )
                )
        return {iid: tuple(item_edges) for iid, item_edges in edges.items()}


@dataclass
class PagesResult:
    item_id: str
    rows: list[dict]


class PagesPhase(Phase):
    """Fetch IIIF manifests and page text for a *sample* of issues.

    A sample, deliberately: the full corpus is ~15.7M pages, ~16.7M requests
    (one per page; the IIIF annotation API has no batch form) and ~950 GB, of
    which 90% is the word-level annotations column, not the text.
    """

    name = "pages"
    DONE_LIST: ClassVar[str] = "pages_done.txt"
    SAMPLE_FILE: ClassVar[str] = "pages_sample.json"

    _pbar: tqdm | None = None

    @property
    def _state(self) -> PagesState:
        return self._checkpoint.pages

    @property
    def _finalized(self) -> bool:
        return self._state.finalized

    @cached_property
    def _parser(self) -> PageParser:
        return PageParser(self._errors)

    async def _execute(self) -> None:
        items_path = self._store.output_dir / "items.parquet"
        if not items_path.exists():
            raise click.ClickException("pages: run the items phase first")

        sample = self._load_or_build_sample(items_path)
        done = self._store.read_lines(self.DONE_LIST)
        todo = [entry for entry in sample if entry["item_id"] not in done]
        if len(todo) < len(sample):
            self._echo(f"resuming: {len(sample) - len(todo)}/{len(sample)} items already harvested")
        self._results: list[PagesResult] = []
        self._pbar = tqdm(
            desc="pages", unit="item", total=len(sample), initial=len(sample) - len(todo)
        )
        try:
            failures = await self._run_pipeline(
                todo, self._harvest_item, self._write_result, self._flush
            )
        finally:
            self._pbar.close()
        self._log_failures(failures)
        if failures:
            raise click.ClickException(
                f"pages: {len(failures)} items hit unexpected errors; re-run to resume"
            )

        n_pages, n_items = self._store.merge_sharded(
            "pages_*.parquet",
            "pages",
            Schemas.PAGES,
            self._settings.pages_split_bytes,
            distinct_column="item_id",
        )
        self._state.finalized = True
        self._checkpoint.save()
        self._metadata.update(
            "pages",
            {
                "sampled_items": len(sample),
                "items_with_pages": n_items,
                "pages": n_pages,
            },
        )
        self._echo(
            f"wrote {Fmt.count(n_pages)} pages from {n_items}/{len(sample)} sampled items"
        )

    def _load_or_build_sample(self, items_path: Path) -> list[dict]:
        path = self._store.parts_dir / self.SAMPLE_FILE
        if path.exists():
            stored = json.loads(path.read_text(encoding="utf-8"))
            # Reuse the stored sample only if both knobs still match, otherwise
            # the run would resume against a sample that is not the one asked for.
            if (stored.get("sample_size"), stored.get("strategy")) == (
                self._settings.sample_size,
                self._settings.sample_strategy,
            ):
                return stored["sample"]
        self._echo(
            f"building {self._settings.sample_strategy} sample of "
            f"{self._settings.sample_size} items ..."
        )
        sample = Sampler(self._settings.sample_size, self._settings.sample_strategy).build(
            items_path
        )
        path.write_text(
            json.dumps(
                {
                    "sample_size": self._settings.sample_size,
                    "strategy": self._settings.sample_strategy,
                    "sample": sample,
                }
            ),
            encoding="utf-8",
        )
        return sample

    async def _harvest_item(
        self, entry: dict, emit: Callable[[PagesResult], Awaitable[None]]
    ) -> None:
        rows = await self._fetch_pages(entry["item_id"], entry["manifest_url"])
        self._pbar.update(1)
        await emit(PagesResult(entry["item_id"], rows))

    async def _fetch_pages(self, item_id: str, manifest_url: str) -> list[dict]:
        manifest = await self._api.manifest(manifest_url, item_id)
        if manifest is None:
            return []
        canvases = manifest.get("items") or []
        if not canvases:
            # Some issues have a manifest but no canvases at all (an upstream
            # data gap, not a fetch failure). Without this they would drop out
            # of the sample silently.
            self._errors.log("pages", item_id, f"manifest has no canvases: {manifest_url}")
            return []
        rows: list[dict] = []
        # Canvases are fetched serially: page order stays trivial, and it is
        # the concurrency across sampled items that saturates the rate limit.
        for page_number, canvas in enumerate(canvases, start=1):
            try:
                row = await self._fetch_page(item_id, page_number, canvas)
            except Exception as exc:
                self._errors.log(
                    "pages", item_id, f"canvas {page_number}: {type(exc).__name__}: {exc}"
                )
                continue
            if row is not None:
                rows.append(row)
        return rows

    async def _fetch_page(self, item_id: str, page_number: int, canvas: dict) -> dict | None:
        annotation_refs = canvas.get("annotations") or []
        annopage_url = annotation_refs[0].get("id") if annotation_refs else None
        if not annopage_url:
            self._errors.log("pages", item_id, f"canvas {page_number}: no annotations reference")
            return None
        body = {}
        canvas_items = canvas.get("items") or []
        if canvas_items:
            painting_annos = canvas_items[0].get("items") or []
            if painting_annos:
                body = painting_annos[0].get("body") or {}
        data = await self._api.annopage(annopage_url, item_id)
        if data is None:
            return None
        page = self._parser.parse(data, annopage_url, item_id)
        if page is None:
            return None
        return {
            "item_id": item_id,
            "page_number": page_number,
            "page_id": page["page_id"],
            "text": page["text"],
            "image_url": body.get("id"),
            "image_mime_type": body.get("format"),
            "annotations": page["annotations_json"],
            "language": page["language"],
            "image_width": canvas.get("width"),
            "image_height": canvas.get("height"),
            "text_length": len(page["text"]),
            "text_rights": page["text_rights"],
        }

    async def _write_result(self, msg: PagesResult) -> None:
        self._results.append(msg)
        if len(self._results) >= self._settings.pages_batch:
            await self._flush()

    async def _flush(self) -> None:
        rows = [row for result in self._results for row in result.rows]
        if rows:
            name = f"pages_{self._state.part:05d}.parquet"
            await asyncio.to_thread(self._store.write_part, name, rows, Schemas.PAGES)
            self._state.part += 1
        if self._results:
            self._store.append_lines(self.DONE_LIST, [r.item_id for r in self._results])
            self._results = []
        self._checkpoint.save()


# ---------------------------------------------------------------------------
# Pipeline and CLI
# ---------------------------------------------------------------------------


class BuildPipeline:
    """Wires the shared services together and runs the requested phases in order."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        settings.output_dir.mkdir(parents=True, exist_ok=True)
        settings.parts_dir.mkdir(exist_ok=True)
        settings.cache_dir.mkdir(parents=True, exist_ok=True)
        self.errors = ErrorLog(settings.output_dir / "errors.log")
        self.checkpoint = Checkpoint.load(settings.output_dir / "checkpoint.json")
        self.metadata = MetadataFile(
            settings.output_dir / "metadata.json", settings, self.errors
        )
        self.store = ParquetStore(settings.output_dir, settings.parts_dir)

    async def run(self) -> None:
        phase_types: dict[PhaseName, type[Phase]] = {
            PhaseName.ITEMS: ItemsPhase,
            PhaseName.ENTITIES: EntitiesPhase,
            PhaseName.PAGES: PagesPhase,
        }
        started = perf_counter()
        async with EuropeanaApi(self.settings, self.errors) as api:
            for name in self.settings.phases:
                phase = phase_types[name](
                    self.settings, api, self.checkpoint, self.store, self.errors, self.metadata
                )
                await phase.run()
        self._print_summary(perf_counter() - started)

    def _print_summary(self, elapsed: float) -> None:
        click.echo(f"build finished in {Fmt.duration(elapsed)} -- {self.settings.output_dir}:")
        for path in sorted(self.settings.output_dir.glob("*.parquet")):
            click.echo(f"  {path.name:<24} {Fmt.size(path.stat().st_size):>10}")
        skipped = self.errors.count()
        if skipped:
            click.echo(f"  {Fmt.count(skipped)} records skipped -> {self.errors.path}")


@click.command()
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("data/output"),
    show_default=True,
    help="Output directory for Parquet files",
)
@click.option(
    "--cache-dir",
    type=click.Path(path_type=Path),
    default=Path("data/cache/http"),
    show_default=True,
    help="HTTP cache directory",
)
@click.option("--refresh-cache", is_flag=True, help="Clear the HTTP cache before starting")
@click.option(
    "--phase",
    type=click.Choice([p.value for p in PhaseName] + ["all"]),
    default="all",
    show_default=True,
    help="Run a specific phase or all",
)
@click.option(
    "--sample-size",
    type=click.IntRange(min=1),
    default=1000,
    show_default=True,
    help="Total number of issues to harvest page text for",
)
@click.option(
    "--sample-strategy",
    type=click.Choice([s.value for s in SampleStrategy]),
    default=SampleStrategy.PROPORTIONAL.value,
    show_default=True,
    help=(
        "proportional: each dataset's share mirrors the corpus (representative); "
        "balanced: every dataset gets an equal share (diverse)"
    ),
)
@click.option(
    "--rate-limit",
    type=click.IntRange(min=1),
    default=5,
    show_default=True,
    help="Max requests per second sent to the Europeana APIs",
)
@click.option(
    "--max-partitions",
    type=click.IntRange(min=0),
    default=0,
    help="Testing: harvest only the first N year partitions (0 = all)",
)
@click.option(
    "--max-requests",
    type=click.IntRange(min=0),
    default=0,
    help="Testing: cap search requests per year partition (0 = unlimited)",
)
def main(
    output_dir: Path,
    cache_dir: Path,
    refresh_cache: bool,
    phase: str,
    sample_size: int,
    sample_strategy: str,
    rate_limit: int,
    max_partitions: int,
    max_requests: int,
) -> None:
    """Harvest the Europeana Open Newspapers dataset into Parquet files."""
    load_dotenv()
    if refresh_cache and cache_dir.exists():
        shutil.rmtree(cache_dir)
        click.echo(f"cleared HTTP cache at {cache_dir}")

    settings = Settings.from_cli(
        output_dir=output_dir,
        cache_dir=cache_dir,
        phases=tuple(PhaseName) if phase == "all" else (PhaseName(phase),),
        sample_size=sample_size,
        sample_strategy=SampleStrategy(sample_strategy),
        rate_limit=rate_limit,
        max_partitions=max_partitions,
        max_requests=max_requests,
    )
    if not settings.api_key and any(
        p in (PhaseName.ITEMS, PhaseName.ENTITIES) for p in settings.phases
    ):
        raise click.ClickException(
            "EUROPEANA_API_KEY is not set (export it or add it to a .env file)"
        )
    asyncio.run(BuildPipeline(settings).run())


if __name__ == "__main__":
    main()
