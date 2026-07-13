# Europeana Open Newspapers

Builds a dataset of openly licensed historical newspapers with OCR full text, harvested
from Europeana's public APIs into Parquet files.

The corpus is roughly **995,000 newspaper issues** published between **1600 and 1950**,
contributed by 13 European libraries. Every issue carries an open licence and OCR text
that Europeana has ingested into its Fulltext API — which is what makes page-level text
and word-level annotations (with bounding boxes) retrievable.

## Quick start

```bash
# Europeana API key: https://pro.europeana.eu/pages/get-api
echo 'EUROPEANA_API_KEY=your-key' > .env

./build.py                      # full harvest into data/output
```

`build.py` is a [uv](https://docs.astral.sh/uv/) script — the shebang installs its own
dependencies on first run, so there is no virtualenv or `pip install` step.

## Output

| file | one row per | notes |
| --- | --- | --- |
| `items.parquet` | newspaper issue | title, language, country, publication year, provider, rights, IIIF manifest URL |
| `pages.parquet` | page | OCR text, image URL and dimensions, word-level annotations as JSON |
| `enrichments.parquet` | item→entity edge | which entity each item links to, and via which property |
| `entities.parquet` | entity fact | labels, coordinates, date ranges for concepts, agents, places and timespans |

Alongside them: `sample_metadata.json` (counts, harvest date, endpoints used),
`errors.log` (skipped records, one per line) and `checkpoint.json` (resume state).

`pages.parquet` is sharded at ~1 GB — later shards are `pages_001.parquet`,
`pages_002.parquet`, and so on.

## Options

```
--phase [items|entities|pages|all]   Run one phase or all three   [default: all]
--max-items INTEGER                  Items per dataset for the page sample  [default: 100]
--rate-limit INTEGER                 Requests per second to the Europeana APIs  [default: 5]
--output-dir PATH                    [default: data/output]
--cache-dir PATH                     [default: data/cache/http]
--refresh-cache                      Clear the HTTP cache before starting
```

The three phases run in order and each depends on the last:

1. **items** — harvests every issue's metadata from the Fulltext Search API. Runs one
   cursor chain per publication year, concurrently, so this phase is bounded by
   `--rate-limit` rather than by round-trip latency. ~10,000 requests.
2. **entities** — resolves the concept, agent, place and timespan URIs the items link
   to via the Entity API, then fills the convenience columns on `items.parquet`
   (`dc_subject_en`, `enriched_places`, …).
3. **pages** — fetches IIIF manifests and annotation pages for a **sample** of items:
   up to `--max-items` per dataset, round-robined across `dc_type` so the sample isn't
   dominated by one kind of record. With 12 datasets, `--max-items 25` gives ~300
   issues.

Note that **pages is sampled, not exhaustive** — full text for all 995k issues would be
millions of requests. Raise `--max-items` for a bigger text corpus.

## Resuming

The harvest is checkpointed and safe to interrupt. Progress is saved every 200 requests
per year partition, and every HTTP response is cached in `data/cache/http` and never
expires. Re-running the same command picks up where it left off; anything already
fetched is replayed from cache rather than refetched.

To start over, delete the output directory (`checkpoint.json` marks finished phases as
finalized, so they are otherwise skipped).

## What is and isn't included

- **Open licences only** (`reusability=open`), so the text and images are reusable.
- **Newspaper collections only.** The `theme=newspaper` query also returns a few records
  from non-newspaper collections (a 1989 photo archive, for instance, whose items are
  tagged with the type "Newspaper" but have no OCR). Items are kept only if their
  dataset name contains "Newspapers".
- **Date-bearing items only.** Every item must have a `dcterms:issued` date. This drops
  exactly one record from ~995k, and in exchange the whole dataset is filterable by year.

## Licence

The harvested content is openly licensed, but licences vary per item and per page — see
the `image_rights` column in `items.parquet` and `text_rights` in `pages.parquet`.
Attribution requirements are the contributing institution's, not this repository's.
