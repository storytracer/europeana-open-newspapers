#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "click",
#     "hishel<1",
#     "httpx",
#     "pyarrow",
#     "pyrate-limiter>=4",
#     "tqdm",
#     "python-dotenv",
# ]
# ///
"""Build the Europeana Open Newspapers dataset.

Harvests openly licensed newspapers with OCR full text from Europeana's
Fulltext Search, Entity and IIIF APIs and writes items.parquet, enrichments.parquet,
entities.parquet and pages.parquet.

Phases: items -> entities -> pages (all three run with --phase all).
"""

import asyncio
import json
import logging
import math
import os
import re
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import click
import hishel
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyrate_limiter import Duration, limiter_factory
from pyrate_limiter.extras.httpx_limiter import AsyncRateLimiterTransport
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The Fulltext Search API, not record/v2/search.json. Only records ingested into
# Europeana's Fulltext API are served here, and only those get IIIF AnnotationPages
# (i.e. the pages phase). The Search API's text_fulltext=true flag is a different,
# largely disjoint set: it marks records whose media file is text-searchable (e.g. a
# PDF with a text layer), whose OCR was never ingested and has no annotations.
FULLTEXT_SEARCH_API = "https://api.europeana.eu/fulltext/search.json"
ENTITY_API = "https://api.europeana.eu/entity"
IIIF_PRESENTATION = "https://iiif.europeana.eu/presentation"
DATA_EUROPEANA_PREFIX = "http://data.europeana.eu/"
ITEM_URI_PREFIX = "http://data.europeana.eu/item"

SEARCH_PARAMS = {
    "query": "*",
    "reusability": "open",
    "qf": "TYPE:TEXT",
    "theme": "newspaper",
    "rows": "100",
    "profile": "rich",
}

# The theme=newspaper query also returns a handful of records from collections that
# are not newspaper collections at all (e.g. 135_Ag_EU_1989_Germany, a crowdsourced
# 1989 photo archive whose items carry dc_type "Newspaper" but have no OCR). Every
# genuine newspaper dataset has "Newspapers" in its edm_datasetName, so require it.
NEWSPAPER_DATASET_SUBSTRING = "newspapers"

RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
CONCURRENCY = 8
RATE_LIMIT = 5  # requests per second, across all APIs; override with --rate-limit
ITEMS_FLUSH_EVERY = 200  # search requests per part-file flush
ENTITY_CHUNK = 500
PAGES_CHUNK = 25
PAGES_SPLIT_BYTES = 1 << 30

# Hidden testing hook: caps the number of cursor requests per Search API loop
# so the whole pipeline can be exercised on a small corpus. Not a CLI flag.
MAX_REQUESTS_ENV = "EOT_MAX_REQUESTS"

ENTITY_URI_RE = re.compile(
    r"^http://data\.europeana\.eu/(concept|agent|place|timespan)/(?:base/)?(\d+)$"
)
URI_TYPE_TO_CLASS = {
    "concept": "skos_Concept",
    "agent": "edm_Agent",
    "place": "edm_Place",
    "timespan": "edm_TimeSpan",
}
CHAR_RE = re.compile(r"#char=(\d+),(\d+)")
XYWH_RE = re.compile(r"#xywh=(\d+),(\d+),(\d+),(\d+)")

ITEMS_SCHEMA = pa.schema(
    [
        ("item_id", pa.string()),
        ("parent_id", pa.string()),
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
        ("language", pa.string()),
        ("country", pa.string()),
        ("edm_year", pa.string()),
        ("data_provider", pa.string()),
        ("provider", pa.string()),
        ("dataset_name", pa.string()),
        ("manifest_url", pa.string()),
        ("europeana_url", pa.string()),
        ("image_rights", pa.string()),
        ("theme", pa.string()),
    ]
)

PAGES_SCHEMA = pa.schema(
    [
        ("item_id", pa.string()),
        ("page_number", pa.int16()),
        ("page_id", pa.string()),
        ("text", pa.string()),
        ("image_url", pa.string()),
        ("image_mime_type", pa.string()),
        ("annotations", pa.string()),
        ("language", pa.string()),
        ("image_width", pa.int32()),
        ("image_height", pa.int32()),
        ("text_length", pa.int32()),
        ("text_rights", pa.string()),
    ]
)

ENRICHMENTS_SCHEMA = pa.schema(
    [
        ("item_id", pa.string()),
        ("entity_uri", pa.string()),
        ("entity_class", pa.string()),
        ("source_property", pa.string()),
    ]
)

ENTITIES_SCHEMA = pa.schema(
    [
        ("entity_uri", pa.string()),
        ("entity_class", pa.string()),
        ("field", pa.string()),
        ("value", pa.string()),
        ("language", pa.string()),
    ]
)

# ---------------------------------------------------------------------------
# Error logging
# ---------------------------------------------------------------------------

_error_logger: logging.Logger | None = None


def setup_error_log(output_dir: Path) -> None:
    global _error_logger
    logger = logging.getLogger("eot.errors")
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.FileHandler(output_dir / "errors.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s\t%(message)s"))
        logger.addHandler(handler)
    _error_logger = logger


def log_error(phase: str, record_id: str, message: str) -> None:
    if _error_logger is not None:
        _error_logger.error("%s\t%s\t%s", phase, record_id, message)


def count_errors(output_dir: Path) -> int:
    path = output_dir / "errors.log"
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as fh:
        return sum(1 for _ in fh)


# ---------------------------------------------------------------------------
# Checkpoint and small-file helpers
# ---------------------------------------------------------------------------


def load_checkpoint(output_dir: Path) -> dict:
    path = output_dir / "checkpoint.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_checkpoint(output_dir: Path, ckpt: dict) -> None:
    path = output_dir / "checkpoint.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(ckpt, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def append_lines(path: Path, lines: list[str]) -> None:
    if not lines:
        return
    with path.open("a", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")


def read_lines(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as fh:
        return {line.rstrip("\n") for line in fh if line.strip()}


def chunks(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def update_metadata(output_dir: Path, section: str, values: dict) -> None:
    path = output_dir / "sample_metadata.json"
    meta = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    meta["harvest_date"] = datetime.now(timezone.utc).isoformat()
    meta["endpoints"] = {
        "fulltext_search_api": FULLTEXT_SEARCH_API,
        "entity_api": ENTITY_API + "/{type}/{id}",
        "iiif_presentation_api": IIIF_PRESENTATION + "/{record_id}/manifest?format=3",
        "iiif_fulltext_api": "{annopage_url}?profile=text",
    }
    meta.setdefault("counts", {})[section] = values
    meta["errors_skipped"] = count_errors(output_dir)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


def make_client(cache_dir: Path, rate_limit: int) -> httpx.AsyncClient:
    limiter = limiter_factory.create_inmemory_limiter(
        rate_per_duration=rate_limit, duration=Duration.SECOND
    )
    rate_limited = AsyncRateLimiterTransport(limiter=limiter)
    storage = hishel.AsyncFileStorage(base_path=cache_dir)  # ttl=None -> never expires
    controller = hishel.Controller(force_cache=True, cacheable_methods=["GET"])
    cached = hishel.AsyncCacheTransport(
        transport=rate_limited, controller=controller, storage=storage
    )
    return httpx.AsyncClient(
        transport=cached,
        timeout=httpx.Timeout(60.0),
        follow_redirects=True,
        headers={"User-Agent": "europeana-open-newspapers-build/1.0"},
    )


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    phase: str = "",
    record_id: str = "",
) -> dict | None:
    """GET a JSON resource with retries; log and return None on final failure.

    Error responses (4xx/5xx) are not cached by hishel (only 200/301/308 are),
    so retries always reach the network.
    """
    last_err = None
    for attempt in range(MAX_ATTEMPTS):
        if attempt:
            await asyncio.sleep(min(2**attempt, 30))
        try:
            resp = await client.get(url, params=params, headers=headers)
        except httpx.HTTPError as exc:
            last_err = f"{type(exc).__name__}: {exc}"
            continue
        if resp.status_code in RETRY_STATUSES:
            last_err = f"HTTP {resp.status_code}"
            continue
        if resp.status_code != 200:
            log_error(phase, record_id, f"HTTP {resp.status_code} for {url}")
            return None
        try:
            return resp.json()
        except ValueError as exc:
            last_err = f"invalid JSON: {exc}"
            continue
    log_error(phase, record_id, f"failed after {MAX_ATTEMPTS} attempts for {url}: {last_err}")
    return None


# ---------------------------------------------------------------------------
# Parquet helpers
# ---------------------------------------------------------------------------


def write_part(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="snappy")


def merge_parts(part_paths: list[Path], out_path: Path, schema: pa.Schema) -> int:
    """Stream part files row-group-by-row-group into a single Parquet file."""
    tmp = out_path.with_suffix(".parquet.tmp")
    n_rows = 0
    with pq.ParquetWriter(tmp, schema, compression="snappy") as writer:
        if not part_paths:
            writer.write_table(pa.Table.from_pylist([], schema=schema))
        for part in part_paths:
            pf = pq.ParquetFile(part)
            for rg in range(pf.num_row_groups):
                table = pf.read_row_group(rg)
                if table.num_rows:
                    writer.write_table(table)
                    n_rows += table.num_rows
    os.replace(tmp, out_path)
    return n_rows


def iter_item_batches(path: Path, columns: list[str] | None = None, batch_size: int = 10_000):
    pf = pq.ParquetFile(path)
    yield from pf.iter_batches(batch_size=batch_size, columns=columns)


# ---------------------------------------------------------------------------
# Phase: items
# ---------------------------------------------------------------------------


def dumps_or_none(value) -> str | None:
    return json.dumps(value, ensure_ascii=False) if value else None


def first_or_none(values):
    return values[0] if values else None


def entity_class_for(uri: str, fallback: str) -> str:
    m = ENTITY_URI_RE.match(uri)
    return URI_TYPE_TO_CLASS[m.group(1)] if m else fallback


def is_newspaper_dataset(dataset_name: str | None) -> bool:
    return bool(dataset_name) and NEWSPAPER_DATASET_SUBSTRING in dataset_name.lower()


def extract_item(item: dict) -> tuple[dict, list[dict], list[tuple[str, str]]]:
    """One Search API item -> (item row, enrichment edge rows, third-party label hints)."""
    iid = item["id"]
    item_uri = ITEM_URI_PREFIX + iid

    row = {
        "item_id": item_uri,
        "parent_id": next(
            (
                v
                for v in item.get("dctermsIsPartOf", [])
                if isinstance(v, str) and v.startswith("http://")
            ),
            None,
        ),
        "dc_title": json.dumps(item.get("dcTitleLangAware", {}), ensure_ascii=False),
        "dc_description": dumps_or_none(item.get("dcDescriptionLangAware")),
        "dc_type": dumps_or_none(item.get("dcTypeLangAware")),
        "dc_type_en": None,
        "dc_subject_en": None,
        "dc_creator_en": None,
        "enriched_concepts": None,
        "enriched_agents": None,
        "enriched_places": None,
        "enriched_timespans": None,
        "language": first_or_none(item.get("language", [])),
        "country": first_or_none(item.get("country", [])),
        "edm_year": dumps_or_none(item.get("year")),
        "data_provider": first_or_none(item.get("dataProvider", [])),
        "provider": first_or_none(item.get("provider", [])),
        "dataset_name": first_or_none(item.get("edmDatasetName", [])),
        "manifest_url": f"{IIIF_PRESENTATION}{iid}/manifest",
        "europeana_url": f"https://www.europeana.eu/item{iid}",
        "image_rights": first_or_none(item.get("rights", [])),
        "theme": "newspaper",  # the query is restricted to theme=newspaper
    }

    def def_uris(key: str) -> list[str]:
        values = (item.get(key) or {}).get("def") or []
        return [v for v in values if isinstance(v, str) and v.startswith("http://")]

    edges: dict[str, tuple[str, str]] = {}  # uri -> (entity_class, source_property)
    for key, source, fallback in (
        ("dcTypeLangAware", "dc_type", "skos_Concept"),
        ("dcSubjectLangAware", "dc_subject", "skos_Concept"),
        ("dcCreatorLangAware", "dc_creator", "edm_Agent"),
    ):
        for uri in def_uris(key):
            edges.setdefault(uri, (entity_class_for(uri, fallback), source))
    for key, source, fallback in (
        ("edmConcept", "edm_concept", "skos_Concept"),
        ("edmAgent", "edm_agent", "edm_Agent"),
        ("edmPlace", "dcterms_spatial", "edm_Place"),
        ("edmTimespan", "dcterms_temporal", "edm_TimeSpan"),
    ):
        for uri in item.get(key) or []:
            if isinstance(uri, str) and uri.startswith("http://"):
                edges.setdefault(uri, (entity_class_for(uri, fallback), source))

    edge_rows = [
        {"item_id": item_uri, "entity_uri": uri, "entity_class": cls, "source_property": src}
        for uri, (cls, src) in edges.items()
    ]

    # Third-party entity URIs are not resolvable via the Entity API. Capture an
    # English label only when the mapping is unambiguous: the item has exactly
    # one entity URI of that class (the third-party one) and exactly one
    # English label in the corresponding *LabelLangAware field.
    hints: list[tuple[str, str]] = []
    for cls, label_field in (
        ("skos_Concept", "edmConceptPrefLabelLangAware"),
        ("edm_Agent", "edmAgentLabelLangAware"),
    ):
        class_uris = [u for u, (c, _) in edges.items() if c == cls]
        third_party = [u for u in class_uris if not u.startswith(DATA_EUROPEANA_PREFIX)]
        if len(class_uris) == 1 and len(third_party) == 1:
            en_labels = (item.get(label_field) or {}).get("en") or []
            if len(en_labels) == 1:
                hints.append((third_party[0], en_labels[0]))
    return row, edge_rows, hints


async def search_cursor_loop(
    client: httpx.AsyncClient,
    api_key: str,
    state: dict,
    cursor_key: str,
    requests_key: str,
    done_key: str,
    total_key: str,
    extra_params: dict,
    on_response,
    on_flush,
    desc: str,
    output_dir: Path,
    ckpt: dict,
) -> None:
    """Drive one cursor-paginated Search API loop with periodic flush/checkpoint."""
    max_requests = int(os.environ.get(MAX_REQUESTS_ENV, "0") or 0)
    cursor = state[cursor_key]
    total = state.get(total_key)
    pbar = tqdm(
        desc=desc,
        unit="req",
        initial=state[requests_key],
        total=math.ceil(total / 100) if total else None,
    )
    requests_since_flush = 0
    try:
        while cursor:
            params = dict(SEARCH_PARAMS)
            params.update(extra_params)
            params["cursor"] = cursor
            data = await fetch_json(
                client,
                FULLTEXT_SEARCH_API,
                params=params,
                headers={"x-api-key": api_key},
                phase="items",
                record_id=f"{desc} request #{state[requests_key]}",
            )
            if data is None or data.get("success") is False:
                # Cursor chains cannot skip a page; flush progress and abort.
                on_flush(cursor)
                save_checkpoint(output_dir, ckpt)
                raise click.ClickException(
                    f"{desc}: Search API request failed after retries; re-run to resume"
                )
            if state.get(total_key) is None and data.get("totalResults") is not None:
                state[total_key] = data["totalResults"]
                pbar.total = math.ceil(data["totalResults"] / 100)
            on_response(data.get("items") or [])
            state[requests_key] += 1
            requests_since_flush += 1
            pbar.update(1)
            cursor = data.get("nextCursor")
            if requests_since_flush >= ITEMS_FLUSH_EVERY:
                on_flush(cursor)
                save_checkpoint(output_dir, ckpt)
                requests_since_flush = 0
            if max_requests and state[requests_key] >= max_requests:
                tqdm.write(f"{desc}: stopping early at {MAX_REQUESTS_ENV}={max_requests}")
                cursor = None
        state[done_key] = True
        on_flush(None)
        save_checkpoint(output_dir, ckpt)
    finally:
        pbar.close()


async def phase_items(
    client: httpx.AsyncClient, api_key: str, output_dir: Path, parts_dir: Path, ckpt: dict
) -> None:
    st = ckpt.setdefault(
        "items",
        {
            "cursor": "*",
            "requests": 0,
            "part": 0,
            "done": False,
            "total_results": None,
            "finalized": False,
            "skipped_non_newspaper": 0,
        },
    )
    if st.get("finalized"):
        click.echo("items: already finalized, skipping (delete checkpoint.json to redo)")
        return

    if not st["done"]:
        item_rows: list[dict] = []
        enrich_rows: list[dict] = []
        hint_rows: list[tuple[str, str]] = []

        def on_response(items: list[dict]) -> None:
            for item in items:
                if not is_newspaper_dataset(first_or_none(item.get("edmDatasetName", []))):
                    st["skipped_non_newspaper"] += 1
                    continue
                try:
                    row, edge_rows, hints = extract_item(item)
                except Exception as exc:
                    log_error("items", str(item.get("id")), f"{type(exc).__name__}: {exc}")
                    continue
                item_rows.append(row)
                enrich_rows.extend(edge_rows)
                hint_rows.extend(hints)

        def on_flush(next_cursor: str | None) -> None:
            if item_rows:
                write_part(parts_dir / f"items_{st['part']:05d}.parquet", item_rows, ITEMS_SCHEMA)
                write_part(
                    parts_dir / f"enrich_{st['part']:05d}.parquet", enrich_rows, ENRICHMENTS_SCHEMA
                )
                st["part"] += 1
                item_rows.clear()
                enrich_rows.clear()
            if hint_rows:
                append_lines(
                    parts_dir / "thirdparty_labels.jsonl",
                    [
                        json.dumps({"uri": u, "label": l}, ensure_ascii=False)
                        for u, l in hint_rows
                    ],
                )
                hint_rows.clear()
            st["cursor"] = next_cursor

        await search_cursor_loop(
            client, api_key, st, "cursor", "requests", "done", "total_results",
            {}, on_response, on_flush, "items: search pages", output_dir, ckpt,
        )

    finalize_items(output_dir, parts_dir, st)
    save_checkpoint(output_dir, ckpt)


def finalize_items(output_dir: Path, parts_dir: Path, st: dict) -> None:
    item_parts = sorted(parts_dir.glob("items_*.parquet"))
    seen: set[str] = set()
    dup_ids: set[str] = set()
    institutions: set[str] = set()
    datasets: set[str] = set()
    n_items = 0

    tmp = output_dir / "items.parquet.tmp"
    with pq.ParquetWriter(tmp, ITEMS_SCHEMA, compression="snappy") as writer:
        if not item_parts:
            writer.write_table(pa.Table.from_pylist([], schema=ITEMS_SCHEMA))
        for part in tqdm(item_parts, desc="items: merging parts", unit="part"):
            keep = []
            for row in pq.read_table(part).to_pylist():
                iid = row["item_id"]
                if iid in seen:
                    dup_ids.add(iid)
                    continue
                seen.add(iid)
                if row["data_provider"]:
                    institutions.add(row["data_provider"])
                if row["dataset_name"]:
                    datasets.add(row["dataset_name"])
                keep.append(row)
                n_items += 1
            if keep:
                writer.write_table(pa.Table.from_pylist(keep, schema=ITEMS_SCHEMA))
    os.replace(tmp, output_dir / "items.parquet")

    # Merge enrichment edges; deduplicate only edges belonging to duplicated items.
    enrich_parts = sorted(parts_dir.glob("enrich_*.parquet"))
    dup_edges_seen: set[tuple] = set()
    n_edges = 0
    tmp = output_dir / "enrichments.parquet.tmp"
    with pq.ParquetWriter(tmp, ENRICHMENTS_SCHEMA, compression="snappy") as writer:
        if not enrich_parts:
            writer.write_table(pa.Table.from_pylist([], schema=ENRICHMENTS_SCHEMA))
        for part in enrich_parts:
            keep = []
            for row in pq.read_table(part).to_pylist():
                if row["item_id"] in dup_ids:
                    key = (row["item_id"], row["entity_uri"], row["source_property"])
                    if key in dup_edges_seen:
                        continue
                    dup_edges_seen.add(key)
                keep.append(row)
                n_edges += 1
            if keep:
                writer.write_table(pa.Table.from_pylist(keep, schema=ENRICHMENTS_SCHEMA))
    os.replace(tmp, output_dir / "enrichments.parquet")

    st["finalized"] = True
    skipped = st.get("skipped_non_newspaper", 0)
    update_metadata(
        output_dir,
        "items",
        {
            "items": n_items,
            "institutions": len(institutions),
            "datasets": len(datasets),
            "enrichment_edges": n_edges,
            "skipped_non_newspaper_datasets": skipped,
        },
    )
    click.echo(
        f"items: wrote {n_items} items ({len(institutions)} institutions, "
        f"{len(datasets)} datasets, {n_edges} enrichment edges; "
        f"{skipped} skipped as non-newspaper datasets)"
    )


# ---------------------------------------------------------------------------
# Phase: entities
# ---------------------------------------------------------------------------


def _as_list(value) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _get(data: dict, *keys):
    for key in keys:
        if data.get(key) is not None:
            return data[key]
    return None


def entity_rows(uri: str, entity_class: str, data: dict) -> list[dict]:
    rows: list[dict] = []

    def add(field: str, value, language: str | None = None) -> None:
        if value is None:
            return
        rows.append(
            {
                "entity_uri": uri,
                "entity_class": entity_class,
                "field": field,
                "value": str(value),
                "language": language,
            }
        )

    for field in ("prefLabel", "altLabel"):
        labels = _get(data, field, f"skos:{field}") or {}
        if isinstance(labels, dict):
            for lang, values in labels.items():
                for value in _as_list(values):
                    add(field, value, lang)

    for field, keys in (
        ("broader", ("broader", "skos:broader")),
        ("narrower", ("narrower", "skos:narrower")),
        ("sameAs", ("sameAs", "owl:sameAs")),
        ("exactMatch", ("exactMatch", "skos:exactMatch")),
    ):
        for value in _as_list(_get(data, *keys)):
            if isinstance(value, dict):
                value = value.get("id") or value.get("@id")
            add(field, value)

    for field, keys in (
        ("lat", ("lat", "latitude", "wgs84_pos:lat")),
        ("long", ("long", "longitude", "wgs84_pos:long")),
        ("begin", ("begin", "edm:begin")),
        ("end", ("end", "edm:end")),
        ("dateOfBirth", ("dateOfBirth", "rdaGr2:dateOfBirth")),
        ("dateOfDeath", ("dateOfDeath", "rdaGr2:dateOfDeath")),
    ):
        for value in _as_list(_get(data, *keys)):
            if isinstance(value, dict):
                value = value.get("id") or value.get("@id")
            add(field, value)
    return rows


async def phase_entities(
    client: httpx.AsyncClient, api_key: str, output_dir: Path, parts_dir: Path, ckpt: dict
) -> None:
    enrichments_path = output_dir / "enrichments.parquet"
    items_path = output_dir / "items.parquet"
    if not enrichments_path.exists() or not items_path.exists():
        raise click.ClickException("entities: run the items phase first")

    st = ckpt.setdefault("entities", {"part": 0, "finalized": False, "post_done": False})
    if st.get("finalized") and st.get("post_done"):
        click.echo("entities: already finalized, skipping")
        return

    if not st["finalized"]:
        uris = sorted(
            set(pq.read_table(enrichments_path, columns=["entity_uri"])["entity_uri"].to_pylist())
        )
        resolvable: list[tuple[str, str, str]] = []
        for uri in uris:
            if not uri.startswith(DATA_EUROPEANA_PREFIX):
                continue  # third-party URI: not resolvable via the Entity API
            m = ENTITY_URI_RE.match(uri)
            if not m:
                log_error("entities", uri, "unparseable Europeana entity URI")
                continue
            resolvable.append((uri, m.group(1), m.group(2)))

        done_path = parts_dir / "entities_done.txt"
        done = read_lines(done_path)
        todo = [t for t in resolvable if t[0] not in done]
        pbar = tqdm(
            desc="entities", unit="entity", total=len(resolvable),
            initial=len(resolvable) - len(todo),
        )
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def fetch_one(uri: str, etype: str, eid: str):
            async with semaphore:
                data = await fetch_json(
                    client,
                    f"{ENTITY_API}/{etype}/{eid}",
                    headers={"x-api-key": api_key},
                    phase="entities",
                    record_id=uri,
                )
            pbar.update(1)
            return uri, etype, data

        try:
            for chunk in chunks(todo, ENTITY_CHUNK):
                results = await asyncio.gather(*(fetch_one(*t) for t in chunk))
                rows: list[dict] = []
                done_lines: list[str] = []
                for uri, etype, data in results:
                    done_lines.append(uri)
                    if data is None:
                        continue  # already logged by fetch_json
                    rows.extend(entity_rows(uri, URI_TYPE_TO_CLASS[etype], data))
                if rows:
                    write_part(parts_dir / f"entities_{st['part']:05d}.parquet", rows, ENTITIES_SCHEMA)
                    st["part"] += 1
                append_lines(done_path, done_lines)
                save_checkpoint(output_dir, ckpt)
        finally:
            pbar.close()

        n_facts = merge_parts(
            sorted(parts_dir.glob("entities_*.parquet")),
            output_dir / "entities.parquet",
            ENTITIES_SCHEMA,
        )
        st["finalized"] = True
        save_checkpoint(output_dir, ckpt)
        update_metadata(
            output_dir,
            "entities",
            {"resolvable_entities": len(resolvable), "entity_facts": n_facts},
        )
        click.echo(f"entities: resolved {len(resolvable)} entities ({n_facts} facts)")

    if not st["post_done"]:
        postprocess_items(output_dir, parts_dir)
        st["post_done"] = True
        save_checkpoint(output_dir, ckpt)


def postprocess_items(output_dir: Path, parts_dir: Path) -> None:
    """Fill dc_*_en and enriched_* columns in items.parquet from resolved entities."""
    label_en: dict[str, str] = {}
    facts: dict[str, dict] = defaultdict(dict)
    entities_path = output_dir / "entities.parquet"
    if entities_path.exists():
        for batch in iter_item_batches(entities_path):
            for uri, field, value, lang in zip(
                batch["entity_uri"].to_pylist(),
                batch["field"].to_pylist(),
                batch["value"].to_pylist(),
                batch["language"].to_pylist(),
            ):
                if field == "prefLabel" and lang == "en":
                    label_en.setdefault(uri, value)
                elif field in ("lat", "long", "begin", "end"):
                    facts[uri].setdefault("lon" if field == "long" else field, value)

    hints_path = parts_dir / "thirdparty_labels.jsonl"
    if hints_path.exists():
        with hints_path.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    hint = json.loads(line)
                    label_en.setdefault(hint["uri"], hint["label"])

    # All enrichment edges keyed by item; entity URIs are interned via a small
    # cache so repeated URIs share one string object.
    uri_cache: dict[str, str] = {}
    cls_cache: dict[str, str] = {}
    src_cache: dict[str, str] = {}
    edges: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for batch in iter_item_batches(output_dir / "enrichments.parquet"):
        for item_id, uri, cls, src in zip(
            batch["item_id"].to_pylist(),
            batch["entity_uri"].to_pylist(),
            batch["entity_class"].to_pylist(),
            batch["source_property"].to_pylist(),
        ):
            edges[item_id].append(
                (
                    uri_cache.setdefault(uri, uri),
                    cls_cache.setdefault(cls, cls),
                    src_cache.setdefault(src, src),
                )
            )

    def enriched_json(item_edges, cls: str, extra: dict[str, str]) -> str | None:
        objs = []
        for uri, edge_cls, src in item_edges:
            if edge_cls != cls:
                continue
            obj = {"uri": uri, "label_en": label_en.get(uri), "source": src}
            for out_key, fact_key in extra.items():
                obj[out_key] = facts.get(uri, {}).get(fact_key)
            objs.append(obj)
        return json.dumps(objs, ensure_ascii=False) if objs else None

    def labels_for(item_edges, source: str) -> list[str]:
        return [label_en[u] for u, _, s in item_edges if s == source and u in label_en]

    items_path = output_dir / "items.parquet"
    tmp = output_dir / "items.parquet.tmp"
    n_rows = 0
    with pq.ParquetWriter(tmp, ITEMS_SCHEMA, compression="snappy") as writer:
        for batch in tqdm(
            iter_item_batches(items_path), desc="entities: updating items.parquet", unit="batch"
        ):
            rows = batch.to_pylist()
            for row in rows:
                item_edges = edges.get(row["item_id"], [])
                row["dc_type_en"] = labels_for(item_edges, "dc_type")
                row["dc_subject_en"] = labels_for(item_edges, "dc_subject")
                row["dc_creator_en"] = labels_for(item_edges, "dc_creator")
                row["enriched_concepts"] = enriched_json(item_edges, "skos_Concept", {})
                row["enriched_agents"] = enriched_json(item_edges, "edm_Agent", {})
                row["enriched_places"] = enriched_json(
                    item_edges, "edm_Place", {"lat": "lat", "lon": "lon"}
                )
                row["enriched_timespans"] = enriched_json(
                    item_edges, "edm_TimeSpan", {"begin": "begin", "end": "end"}
                )
            if rows:
                writer.write_table(pa.Table.from_pylist(rows, schema=ITEMS_SCHEMA))
                n_rows += len(rows)
        if n_rows == 0:
            writer.write_table(pa.Table.from_pylist([], schema=ITEMS_SCHEMA))
    os.replace(tmp, items_path)
    click.echo(f"entities: updated convenience columns on {n_rows} items")


# ---------------------------------------------------------------------------
# Phase: pages
# ---------------------------------------------------------------------------


def build_sample(items_path: Path, max_items: int) -> list[dict]:
    """Pick up to max_items per dataset, round-robining across dc_type values.

    Grouped by dataset_name rather than data_provider: dataset_name is a controlled
    identifier assigned at ingest, whereas data_provider is a free-text label with
    spelling variants (the Austrian National Library appears under two names for a
    single dataset, and grouping by provider would hand it a double quota).
    """
    groups: dict[str, dict[str, list[tuple[str, str]]]] = defaultdict(lambda: defaultdict(list))
    for batch in iter_item_batches(
        items_path, columns=["item_id", "dataset_name", "dc_type", "manifest_url"]
    ):
        for iid, dataset, dc_type, manifest_url in zip(
            batch["item_id"].to_pylist(),
            batch["dataset_name"].to_pylist(),
            batch["dc_type"].to_pylist(),
            batch["manifest_url"].to_pylist(),
        ):
            groups[dataset or ""][dc_type or ""].append((iid, manifest_url))

    sample: list[dict] = []
    for dataset in sorted(groups):
        buckets = groups[dataset]
        keys = sorted(buckets)
        for key in keys:
            buckets[key].sort()
        pointers = dict.fromkeys(keys, 0)
        picked = 0
        while picked < max_items:
            progressed = False
            for key in keys:
                bucket = buckets[key]
                pos = pointers[key]
                if pos < len(bucket):
                    iid, manifest_url = bucket[pos]
                    sample.append({"item_id": iid, "manifest_url": manifest_url})
                    pointers[key] = pos + 1
                    picked += 1
                    progressed = True
                    if picked >= max_items:
                        break
            if not progressed:
                break
    return sample


def annopage_id_from_url(url: str) -> str | None:
    segments = urlsplit(url).path.rstrip("/").split("/")
    if "annopage" in segments:
        idx = segments.index("annopage")
        return "/".join(segments[idx + 1 :]) or None
    return segments[-1] or None


async def fetch_annopage(client: httpx.AsyncClient, url: str, item_id: str):
    separator = "&" if "?" in url else "?"
    data = await fetch_json(
        client, f"{url}{separator}profile=text", phase="pages", record_id=item_id
    )
    if data is None:
        return None
    resources = data.get("resources") or []
    page_ann = next((r for r in resources if r.get("textGranularity") == "page"), None)
    if page_ann is None:
        log_error("pages", item_id, f"no page-level annotation in {url}")
        return None
    resource = page_ann.get("resource") or {}
    text = resource.get("value")
    if text is None:
        log_error("pages", item_id, f"page-level annotation without text value in {url}")
        return None
    language = data.get("language") or resource.get("language")
    text_rights = resource.get("edmRights") or data.get("edmRights")

    annotations = []
    for res in resources:
        granularity = res.get("textGranularity")
        if granularity not in ("block", "line", "word"):
            continue
        resource_id = (res.get("resource") or {}).get("@id") or ""
        char_match = CHAR_RE.search(resource_id)
        on = res.get("on")
        on0 = on[0] if isinstance(on, list) and on else (on if isinstance(on, str) else "")
        bbox_match = XYWH_RE.search(on0 or "")
        char_start, char_end = (
            (int(char_match.group(1)), int(char_match.group(2))) if char_match else (None, None)
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
        "language": language,
        "text_rights": text_rights,
        "annotations_json": json.dumps(annotations, ensure_ascii=False),
        "page_id": annopage_id_from_url(url),
    }


async def harvest_item_pages(
    client: httpx.AsyncClient, item_id: str, manifest_url: str
) -> list[dict]:
    manifest = await fetch_json(
        client, manifest_url, params={"format": "3"}, phase="pages", record_id=item_id
    )
    if manifest is None:
        return []
    rows: list[dict] = []
    for page_number, canvas in enumerate(manifest.get("items") or [], start=1):
        try:
            annotation_refs = canvas.get("annotations") or []
            annopage_url = annotation_refs[0].get("id") if annotation_refs else None
            if not annopage_url:
                log_error("pages", item_id, f"canvas {page_number}: no annotations reference")
                continue
            body = {}
            canvas_items = canvas.get("items") or []
            if canvas_items:
                painting_annos = canvas_items[0].get("items") or []
                if painting_annos:
                    body = painting_annos[0].get("body") or {}
            page = await fetch_annopage(client, annopage_url, item_id)
            if page is None:
                continue
            rows.append(
                {
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
            )
        except Exception as exc:
            log_error("pages", item_id, f"canvas {page_number}: {type(exc).__name__}: {exc}")
    return rows


def finalize_pages(output_dir: Path, parts_dir: Path) -> tuple[int, int]:
    """Merge page parts into shards of roughly PAGES_SPLIT_BYTES each."""
    parts = sorted(parts_dir.glob("pages_*.parquet"))
    shard_index = 0
    writer = None
    tmp_path = None
    bytes_in_shard = 0
    n_pages = 0
    item_ids: set[str] = set()

    def shard_name(index: int) -> Path:
        return output_dir / ("pages.parquet" if index == 0 else f"pages_{index:03d}.parquet")

    def open_writer():
        nonlocal writer, tmp_path
        tmp_path = shard_name(shard_index).with_suffix(".parquet.tmp")
        writer = pq.ParquetWriter(tmp_path, PAGES_SCHEMA, compression="snappy")

    def close_writer():
        nonlocal writer, shard_index, bytes_in_shard
        if writer is not None:
            writer.close()
            os.replace(tmp_path, shard_name(shard_index))
            writer = None
            shard_index += 1
            bytes_in_shard = 0

    open_writer()
    if not parts:
        writer.write_table(pa.Table.from_pylist([], schema=PAGES_SCHEMA))
    for part in parts:
        pf = pq.ParquetFile(part)
        for rg in range(pf.num_row_groups):
            table = pf.read_row_group(rg)
            if not table.num_rows:
                continue
            if writer is None:
                open_writer()
            writer.write_table(table)
            n_pages += table.num_rows
            item_ids.update(table["item_id"].to_pylist())
            bytes_in_shard += table.nbytes
            if bytes_in_shard >= PAGES_SPLIT_BYTES:
                close_writer()
    close_writer()
    return n_pages, len(item_ids)


async def phase_pages(
    client: httpx.AsyncClient, output_dir: Path, parts_dir: Path, ckpt: dict, max_items: int
) -> None:
    items_path = output_dir / "items.parquet"
    if not items_path.exists():
        raise click.ClickException("pages: run the items phase first")

    st = ckpt.setdefault("pages", {"part": 0, "finalized": False})
    if st.get("finalized"):
        click.echo("pages: already finalized, skipping")
        return

    sample_path = parts_dir / "pages_sample.json"
    sample = None
    if sample_path.exists():
        stored = json.loads(sample_path.read_text(encoding="utf-8"))
        if stored.get("max_items") == max_items:
            sample = stored["sample"]
    if sample is None:
        click.echo("pages: building sample from items.parquet ...")
        sample = build_sample(items_path, max_items)
        sample_path.write_text(
            json.dumps({"max_items": max_items, "sample": sample}), encoding="utf-8"
        )

    done_path = parts_dir / "pages_done.txt"
    done = read_lines(done_path)
    todo = [entry for entry in sample if entry["item_id"] not in done]
    pbar = tqdm(desc="pages", unit="item", total=len(sample), initial=len(sample) - len(todo))
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def one(entry: dict):
        async with semaphore:
            rows = await harvest_item_pages(client, entry["item_id"], entry["manifest_url"])
        pbar.update(1)
        return entry["item_id"], rows

    try:
        for chunk in chunks(todo, PAGES_CHUNK):
            results = await asyncio.gather(*(one(entry) for entry in chunk))
            rows = [row for _, item_rows in results for row in item_rows]
            if rows:
                write_part(parts_dir / f"pages_{st['part']:05d}.parquet", rows, PAGES_SCHEMA)
                st["part"] += 1
            append_lines(done_path, [item_id for item_id, _ in results])
            save_checkpoint(output_dir, ckpt)
    finally:
        pbar.close()

    n_pages, n_items_with_pages = finalize_pages(output_dir, parts_dir)
    st["finalized"] = True
    save_checkpoint(output_dir, ckpt)
    update_metadata(
        output_dir,
        "pages",
        {
            "sampled_items": len(sample),
            "items_with_pages": n_items_with_pages,
            "pages": n_pages,
        },
    )
    click.echo(f"pages: wrote {n_pages} pages from {n_items_with_pages}/{len(sample)} sampled items")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


async def run_phases(
    phases: list[str],
    api_key: str | None,
    output_dir: Path,
    cache_dir: Path,
    parts_dir: Path,
    max_items: int,
    rate_limit: int,
) -> None:
    ckpt = load_checkpoint(output_dir)
    async with make_client(cache_dir, rate_limit) as client:
        for phase in phases:
            if phase == "items":
                await phase_items(client, api_key, output_dir, parts_dir, ckpt)
            elif phase == "entities":
                await phase_entities(client, api_key, output_dir, parts_dir, ckpt)
            elif phase == "pages":
                await phase_pages(client, output_dir, parts_dir, ckpt, max_items)


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
    type=click.Choice(["items", "entities", "pages", "all"]),
    default="all",
    show_default=True,
    help="Run a specific phase or all",
)
@click.option(
    "--max-items",
    type=int,
    default=100,
    show_default=True,
    help="Max items per dataset for the page sample",
)
@click.option(
    "--rate-limit",
    type=click.IntRange(min=1),
    default=RATE_LIMIT,
    show_default=True,
    help="Max requests per second sent to the Europeana APIs",
)
def main(
    output_dir: Path,
    cache_dir: Path,
    refresh_cache: bool,
    phase: str,
    max_items: int,
    rate_limit: int,
):
    """Harvest the Europeana Open Newspapers dataset into Parquet files."""
    load_dotenv()
    output_dir.mkdir(parents=True, exist_ok=True)
    parts_dir = output_dir / "parts"
    parts_dir.mkdir(exist_ok=True)
    if refresh_cache and cache_dir.exists():
        shutil.rmtree(cache_dir)
        click.echo(f"cleared HTTP cache at {cache_dir}")
    cache_dir.mkdir(parents=True, exist_ok=True)
    setup_error_log(output_dir)

    phases = ["items", "entities", "pages"] if phase == "all" else [phase]

    api_key = os.environ.get("EUROPEANA_API_KEY")
    if not api_key and any(p in ("items", "entities") for p in phases):
        raise click.ClickException(
            "EUROPEANA_API_KEY is not set (export it or add it to a .env file)"
        )

    asyncio.run(
        run_phases(phases, api_key, output_dir, cache_dir, parts_dir, max_items, rate_limit)
    )


if __name__ == "__main__":
    main()
