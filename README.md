# Europeana Open Newspapers

Builds a dataset of openly licensed historical newspapers with OCR full text, harvested
from Europeana's public APIs into Parquet files.

The corpus is **995,182 newspaper issues** published between **1618 and 1946**,
contributed by 12 European libraries across 11 collections. Every issue carries an open
licence and OCR text that Europeana has ingested into its Fulltext API — which is what
makes page-level text and word-level annotations (with bounding boxes) retrievable.

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
| `items.parquet` | newspaper issue | language, country, `year_issued`, `date_issued`, dataset, title, rights, IIIF manifest URL |
| `pages.parquet` | page | OCR text, image URL and dimensions, word-level annotations as JSON |
| `enrichments.parquet` | item→entity edge | which entity each item links to, and via which property |
| `entities.parquet` | entity fact | labels, coordinates, date ranges for concepts, agents, places and timespans |

Every issue has an exact publication date: `year_issued` (int) and `date_issued` (date).

Alongside them: `metadata.json` (counts, harvest date, endpoints used),
`errors.log` (skipped records, one per line) and `checkpoint.json` (resume state).

`pages.parquet` is sharded at ~1 GB — later shards are `pages_001.parquet`,
`pages_002.parquet`, and so on.

## Options

```
--phase [items|entities|pages|all]        Run one phase or all three   [default: all]
--sample-size INTEGER                     Issues to harvest page text for  [default: 1000]
--sample-strategy [proportional|balanced] Shape of that sample  [default: proportional]
--rate-limit INTEGER                      Requests per second to the APIs  [default: 5]
--output-dir PATH                         [default: data/output]
--cache-dir PATH                          [default: data/cache/http]
--refresh-cache                           Clear the HTTP cache before starting
--max-partitions INTEGER                  Testing: only the first N year partitions
--max-requests INTEGER                    Testing: cap search requests per partition
```

The three phases run in order and each depends on the last:

1. **items** — harvests every issue's metadata from the Fulltext Search API. Runs one
   cursor chain per publication year, concurrently, so this phase is bounded by
   `--rate-limit` rather than by round-trip latency. ~10,400 requests, all 995k issues.
2. **entities** — resolves the concept, agent, place and timespan URIs the items link
   to via the Entity API, then fills the convenience columns on `items.parquet`
   (`dc_subject_en`, `enriched_places`, …).
3. **pages** — fetches IIIF manifests and annotation pages for a **sample** of issues.

## Sampling

Page text is sampled, not exhaustive: the full corpus is ~15.7 million pages and would
take ~16.7 million requests (roughly three weeks at 10 req/s) and ~950 GB. `--sample-size`
is the number of *issues* to fetch text for; each yields ~16 pages.

```bash
./build.py --sample-size 1000                             # representative (default)
./build.py --sample-size 1000 --sample-strategy balanced  # equal per collection
```

| strategy | what it gives you |
| --- | --- |
| `proportional` | each collection's share mirrors its share of the corpus, so the sample is a miniature of the real thing (a third of it Dutch) |
| `balanced` | every collection gets an equal share, so small ones are as visible as large ones — diverse rather than representative |

Under both, the quota is stratified further: proportionally across **decades** (with a
floor, so no period drops out), round-robin across **newspaper titles** (so one paper
cannot swallow a collection's quota), and issues within a title are picked by a hash of
their id rather than in date order. At `--sample-size 1000` either strategy covers all
34 decades, all 11 collections and ~300 distinct newspapers. The sample is deterministic:
the same flags always select the same issues.

## Resuming

The harvest is checkpointed and safe to interrupt. Progress is saved every 200 requests
per year partition, and every HTTP response is cached in `data/cache/http` and never
expires. Re-running the same command picks up where it left off; anything already
fetched is replayed from cache rather than refetched.

To start over, delete the output directory (`checkpoint.json` marks finished phases as
finalized, so they are otherwise skipped).

## What is and isn't included

- **Open licences only** (`reusability=open`), so the text and images are reusable.
- **Newspaper collections only.** Europeana's `theme=newspaper` matches any item *typed*
  "Newspaper", regardless of collection — including, for example, a 1989 photo from a
  crowdsourcing campaign. Items are kept only if their dataset name contains
  "Newspapers".
- **Date-bearing items only.** Every item must have a `dcterms:issued` date. This drops
  exactly one record from ~995k, and in exchange the whole dataset is filterable by date.
- **Some issues have no page text.** A few manifests contain no canvases at all (an
  upstream gap), and some canvases carry no OCR annotation. Both are recorded in
  `errors.log` rather than dropped silently.

## Licence

The harvested content is openly licensed, but licences vary per item and per page — see
the `image_rights` column in `items.parquet` and `text_rights` in `pages.parquet`.
Attribution requirements are the contributing institution's, not this repository's.
