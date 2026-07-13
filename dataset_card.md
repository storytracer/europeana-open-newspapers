---
pretty_name: Europeana Open Newspapers
license: other
license_name: various-open
license_link: https://pro.europeana.eu/page/available-rights-statements
language:
- de
- nl
- lv
- et
- fi
- sr
- pl
- fr
task_categories:
- text-generation
tags:
- newspapers
- ocr
- historical
- multilingual
- europeana
- iiif
- cultural-heritage
- digital-humanities
size_categories:
- 1K<n<10K
configs:
- config_name: pages
  default: true
  data_files:
  - split: train
    path: data/pages/*.parquet
- config_name: items
  data_files:
  - split: train
    path: data/items.parquet
- config_name: enrichments
  data_files:
  - split: train
    path: data/enrichments.parquet
- config_name: entities
  data_files:
  - split: train
    path: data/entities.parquet
- config_name: manifests
  data_files:
  - split: train
    path: data/manifests.parquet
---

# Europeana Open Newspapers

Openly licensed historical newspapers with OCR full text, harvested from
[Europeana](https://www.europeana.eu/)'s public APIs.

The corpus is **995,182 newspaper issues** published between **1618 and 1946**,
contributed by 12 European libraries across 11 collections. Every issue carries an open
licence and OCR text that Europeana has ingested into its Fulltext API. This dataset
contains the **complete issue-level metadata** for the corpus, entity enrichments, and
**page-level OCR text with word-level annotations for a stratified sample of 1,000
issues (6,973 pages)** — page text at full-corpus scale would be ~15.7 million pages
and ~950 GB, so pages are sampled, deliberately and reproducibly.

## Dataset structure

Five configurations, one per Parquet table. The default configuration is `pages`.

| config | file(s) | one row per | rows |
| --- | --- | --- | --- |
| `pages` *(default)* | `data/pages/pages_00001.parquet` … (3 shards) | newspaper page | 6,973 |
| `items` | `data/items.parquet` | newspaper issue | 995,182 |
| `enrichments` | `data/enrichments.parquet` | item→entity edge | 2,465,752 |
| `entities` | `data/entities.parquet` | entity fact | 2,289 |
| `manifests` | `data/manifests.parquet` | sampled issue | 1,000 |

```python
from datasets import load_dataset

pages = load_dataset("storytracer/europeana-open-newspapers", "pages", split="train")
items = load_dataset("storytracer/europeana-open-newspapers", "items", split="train")
```

### `pages` — OCR page text (sample)

| column | type | description |
| --- | --- | --- |
| `item_id` | string | Europeana item URI, joins to `items` |
| `page_number` | int16 | 1-based position of the page within its issue |
| `page_id` | string | identifier of the IIIF annotation page |
| `language` | string | page text language (ISO 639-1) |
| `text` | string | full OCR text of the page |
| `annotations` | string | JSON array of block/line/word annotations (see below) |
| `image_url` | string | URL of the page image (IIIF Image API) |
| `image_mime_type` | string | MIME type of the page image |
| `image_width` | int32 | page image width in pixels |
| `image_height` | int32 | page image height in pixels |
| `text_length` | int32 | length of `text` in characters |
| `text_rights` | string | rights statement URL for the OCR text of this page |

Each element of `annotations` locates a snippet of the page text on the page image:

```json
{"granularity": "word", "text": "Zeitung", "char_start": 1204, "char_end": 1211,
 "bbox_x": 843, "bbox_y": 310, "bbox_w": 164, "bbox_h": 42}
```

`granularity` is `block`, `line` or `word`; `char_start`/`char_end` are offsets into
`text`; the bounding box is in pixels on the page image. Fields are null where the
source annotation lacks them.

### `items` — issue metadata (full corpus)

| column | type | description |
| --- | --- | --- |
| `item_id` | string | Europeana item URI (primary key) |
| `language` | string | issue language (ISO 639-1) |
| `country` | string | providing country |
| `year_issued` | int16 | publication year, derived from the `dcterms:issued` index |
| `date_issued` | date32 | exact publication date, parsed from the title |
| `dataset_name` | string | Europeana collection identifier |
| `europeana_url` | string | human-facing page on europeana.eu |
| `manifest_url` | string | IIIF Presentation manifest URL |
| `dc_title` | string | title labels as JSON, keyed by language |
| `dc_description` | string | description labels as JSON, keyed by language |
| `dc_type` | string | type labels as JSON, keyed by language |
| `dc_type_en` / `dc_subject_en` / `dc_creator_en` | list\<string\> | resolved English labels of typed/subject/creator entities |
| `enriched_concepts` / `enriched_agents` | string | JSON array of linked entities (`uri`, `label_en`, `source`) |
| `enriched_places` | string | as above, plus `lat`/`lon` |
| `enriched_timespans` | string | as above, plus `begin`/`end` |
| `data_provider` | string | contributing institution |
| `provider` | string | aggregator |
| `image_rights` | string | rights statement URL for the issue's images |
| `theme` | string | always `newspaper` |

Every issue has an exact publication date: `year_issued` comes from Europeana's date
index (used to partition the harvest), `date_issued` is parsed from the issue title,
which ends in the date in all 11 collections.

### `enrichments` — item→entity edges (full corpus)

| column | type | description |
| --- | --- | --- |
| `item_id` | string | joins to `items` |
| `entity_uri` | string | entity URI (Europeana or third-party) |
| `entity_class` | string | `skos_Concept`, `edm_Agent`, `edm_Place` or `edm_TimeSpan` |
| `source_property` | string | metadata property the link came from (e.g. `dc_subject`, `dcterms_spatial`) |

### `entities` — resolved entity facts

| column | type | description |
| --- | --- | --- |
| `entity_uri` | string | joins to `enrichments` |
| `entity_class` | string | as above |
| `field` | string | `prefLabel`, `altLabel`, `broader`, `narrower`, `sameAs`, `exactMatch`, `lat`, `long`, `begin`, `end`, `dateOfBirth`, `dateOfDeath` |
| `value` | string | the fact value |
| `language` | string | label language, null for non-label facts |

### `manifests` — raw IIIF manifests (sample)

| column | type | description |
| --- | --- | --- |
| `item_id` | string | joins to `items` and `pages` |
| `manifest_url` | string | where the manifest was fetched from |
| `manifest` | string | the complete IIIF Presentation (v3) manifest as JSON |

Included as provenance: the manifests are the exact source documents the `pages` rows
were derived from.

## Languages

| language | issues | | country | issues |
| --- | --- | --- | --- | --- |
| German (de) | 509,821 | | Germany | 362,303 |
| Dutch (nl) | 325,656 | | Netherlands | 325,656 |
| Latvian (lv) | 68,661 | | Austria | 147,518 |
| Estonian (et) | 28,128 | | Latvia | 68,661 |
| Finnish (fi) | 24,196 | | Estonia | 28,128 |
| Serbian (sr) | 22,273 | | Finland | 24,196 |
| Polish (pl) | 15,130 | | Serbia | 22,273 |
| French (fr) | 1,317 | | Poland | 15,130 |
| | | | Luxembourg | 1,317 |

## How the page sample was drawn

The 1,000 sampled issues are stratified four levels deep, because the corpus is
lopsided in three directions at once (a third of it is one Dutch collection, the
19th–20th centuries dwarf the 17th, and a few large papers dominate each collection):

1. **Collection** — slots are allocated proportionally to each of the 11 collections'
   share of the corpus, so the sample mirrors the real composition.
2. **Decade** — proportional within the collection, with a floor of one issue, so no
   decade disappears.
3. **Newspaper title** — round-robin across titles, so a single large paper cannot
   consume its collection's quota.
4. **Issue** — ordered by an md5 hash of the item id: effectively shuffled, but fully
   deterministic. The same parameters always select the same issues.

The result covers **all 34 decades (1610s–1940s), all 11 collections and ~320 distinct
newspapers**. 889 of the 1,000 sampled issues have retrievable page text (6,973 pages);
the remaining 111 have manifests but no OCR annotations upstream — every skipped record
is accounted for in the build's error log.

**Caveat for statistical use:** this is a *coverage* sample, not a probability sample.
The decade floor and the title round-robin deliberately over-represent rare decades and
small newspapers; corpus-level statistics computed from `pages` should be weighted
accordingly (or computed on `items`, which is complete).

## Dataset creation

Harvested from three public Europeana APIs (2026-07-13):

- **Fulltext Search API** (`api.europeana.eu/fulltext/search.json`) — issue metadata,
  restricted to `theme=newspaper`, `reusability=open`, `TYPE:TEXT`, date-bearing items.
  Only records whose OCR was ingested into Europeana's Fulltext API are served here,
  which is what makes page text retrievable.
- **Entity API** — resolves the concept/agent/place/timespan URIs linked from items.
- **IIIF Presentation & Fulltext APIs** — manifests and page-level annotation pages
  for the sampled issues.

Items typed "Newspaper" that belong to non-newspaper collections (e.g. crowdsourcing
campaigns) are excluded via the collection name; the harvest verifies per-year counts
against the API's own totals before writing anything, so silent filter mismatches fail
loudly rather than shipping a plausible-but-wrong dataset.

The dataset is built by a single self-contained script (`build.py`, Python ≥ 3.11 with
[uv](https://docs.astral.sh/uv/)): `./build.py --sample-size 1000` reproduces it
end-to-end, including the identical sample. The harvest is checkpointed, resumable and
HTTP-cached; `metadata.json` records the harvest date, endpoints and per-table counts.

## Licensing

- **Metadata** (items, enrichments, entities): [CC0](https://creativecommons.org/publicdomain/zero/1.0/),
  per Europeana's Data Exchange Agreement.
- **Content** (page text and images): openly licensed, but the licence **varies per
  item and per page**. The exact rights statement URL is carried on every record:
  `image_rights` in `items`, `text_rights` in `pages`. All of them permit reuse
  (`reusability=open`); attribution requirements are those of the contributing
  institution (`data_provider`).

## Limitations

- **OCR quality varies** with the age and condition of the source material and is not
  manually corrected.
- **Dates are as published by the libraries**: `date_issued` is parsed from the issue
  title; exactly one issue in 995,182 disagrees with the date index (across a New Year
  boundary) and is kept as harvested.
- **Page text is a sample** — see the sampling caveat above. `items` is complete.
- **Coverage reflects Europeana's ingest**, not European newspaper history: eight
  languages and nine countries are represented, with German and Dutch dominant.

## Acknowledgements

The newspapers were digitised and openly licensed by the 12 contributing institutions
(see `data_provider`), aggregated by [Europeana](https://www.europeana.eu/). Dataset
compiled by Sebastian Majstorovic from Europeana's public APIs.
