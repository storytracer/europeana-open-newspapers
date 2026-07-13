# Europeana Open Newspapers — build

Builds the [Europeana Open Newspapers dataset](dataset_card.md): openly licensed
historical newspapers with OCR full text, harvested from Europeana's public APIs into
Parquet files. The corpus is **995,182 newspaper issues** (1618–1946) from 12 European
libraries; page text and word-level annotations are harvested for a stratified,
deterministic sample of issues.

## Quick start

```bash
# Europeana API key: https://pro.europeana.eu/pages/get-api
echo 'EUROPEANA_API_KEY=your-key' > .env

./build.py                      # full harvest into output/
```

`build.py` is a [uv](https://docs.astral.sh/uv/) script — the shebang installs its own
dependencies on first run, so there is no virtualenv or `pip install` step.

## Layout

```
output/                ← --output-dir; the build tree, fixed layout
  dataset/             ← the Hugging Face upload, exactly as it appears on the Hub
    README.md          ← copied from dataset_card.md at the end of each run
    data/              ← the Parquet tables (see the dataset card for schemas)
  work/                ← build state: checkpoint.json, errors.log, parts/
cache/http/            ← --cache-dir; HTTP cache, shared by all builds, never expires
```

`output/dataset/` is the deliverable. Publish it with:

```bash
hf upload <user>/europeana-open-newspapers output/dataset . --repo-type dataset
```

## Options

```
--phase [items|entities|pages|all]        Run one phase or all three   [default: all]
--sample-size INTEGER                     Issues to harvest page text for  [default: 1000]
--sample-strategy [proportional|balanced] Shape of that sample  [default: proportional]
--rate-limit INTEGER                      Requests per second to the APIs  [default: 10]
--workers INTEGER                         Concurrent requests in flight  [default: 24]
--output-dir PATH                         Build tree  [default: output]
--cache-dir PATH                          Shared HTTP cache  [default: cache/http]
--refresh-cache                           Clear the HTTP cache before starting
--max-partitions INTEGER                  Testing: only the first N year partitions
--max-requests INTEGER                    Testing: cap search requests per partition
```

The three phases run in order and each depends on the last:

1. **items** — harvests every issue's metadata from the Fulltext Search API, one
   cursor chain per publication year, concurrently. ~10,400 requests, all 995k issues.
2. **entities** — resolves the linked concept/agent/place/timespan entities, then
   fills the convenience columns on `items.parquet`.
3. **pages** — fetches IIIF manifests and page text for a **sample** of issues
   (`--sample-size`, `--sample-strategy`); manifests are kept in
   `manifests.parquet` as provenance.

Sampling is stratified (collection → decade → title → issue) and deterministic: the
same flags always select the same issues. See the [dataset card](dataset_card.md) for
the full methodology and schemas.

## Resuming and starting over

The harvest is checkpointed and safe to interrupt: re-running the same command resumes
where it left off, and every HTTP response is cached forever, so anything already
fetched replays from disk (a full items rebuild from cache takes ~1½ minutes).

To start over, delete `output/` — stale work state is cleared automatically whenever
the dataset it belongs to is gone. The cache survives; use `--refresh-cache` if you
really want to re-download.

To build a second variant alongside (e.g. the balanced sample), point `--output-dir`
elsewhere; the cache is shared by default:

```bash
./build.py --output-dir output-balanced --sample-strategy balanced
```

## Licence

The harvested content is openly licensed, but licences vary per item and per page —
see `image_rights` in `items.parquet` and `text_rights` in `pages/`. Attribution
requirements are the contributing institutions'; see the
[dataset card](dataset_card.md) for details.
