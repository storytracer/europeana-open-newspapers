---
pretty_name: Europeana Open Newspapers
license: cc0-1.0
language:
- de
- nl
- lv
- et
- fi
- sr
- pl
- fr
- ru
- sv
- it
- en
- es
- he
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

**995,182 openly licensed historical newspaper issues (1618–1946)** from 11 European
libraries, aggregated through [Europeana](https://www.europeana.eu/) — complete
issue-level metadata for the whole corpus, semantic enrichment resolved to the
[Europeana Entity Collection](https://europeana.atlassian.net/wiki/spaces/EF/pages/2324561923),
and **full OCR page text with word-level bounding-box annotations for a stratified,
deterministic sample of 1,000 issues (7,001 pages, ~98 million characters)**.

Every issue in the corpus has OCR text that Europeana has ingested into its Fulltext
API — page text at full-corpus scale would be ~15.7 million pages and ~950 GB, which
is why pages are sampled, deliberately and reproducibly: the same build always selects
the same issues.

> **Snapshot:** Harvested from Europeana's public APIs on 2026-07-13.

## Files

| File | Rows | Size | Description |
|---|---|---|---|
| `data/pages/*.parquet` (3 shards) | 7,001 | 733 MB | Page text + word-level annotations for the sampled issues |
| `data/items.parquet` | 995,182 | 40 MB | One row per issue — the complete corpus metadata |
| `data/enrichments.parquet` | 2,465,752 | 13 MB | Enrichment edges — which entity was linked via which property |
| `data/entities.parquet` | 2,289 | 23 KB | Entity catalogue — labels, coordinates, date ranges |
| `data/manifests.parquet` | 1,000 | 2 MB | Raw IIIF manifests of the sampled issues (provenance) |

`items` is self-contained for corpus-level analysis; `pages` is self-contained for
text work. The auxiliary tables add the linked-data graph and the IIIF provenance.

## What this dataset contains

- Full OCR text for 6,973 newspaper pages, with per-page language, rights statement
  and image facts (URL, dimensions, MIME type)
- Word-, line- and block-level annotations tying every text snippet to character
  offsets and pixel bounding boxes on the page image
- Complete issue-level metadata for all 995,182 issues: multilingual titles, exact
  publication dates, collection, institution, rights
- Europeana's semantic enrichment: concepts, agents, places and timespans resolved to
  the Entity Collection, with English labels, coordinates and date ranges
- The raw IIIF Presentation manifest of every sampled issue

## What this dataset does NOT contain

- **Page text for the full corpus.** Only the 1,000-issue sample carries text; the
  other 994,182 issues are metadata-only. Their text is retrievable via the
  `manifest_url` column, following steps 4–5 of the harvest recipe below.
- **Image bytes.** Use the `image_url` column; images are served by Europeana's IIIF
  image infrastructure.
- **Corrected OCR.** The text is the libraries' OCR as ingested by Europeana, with all
  its period-typical noise.

## Dataset scope

The corpus is defined against Europeana's
[Fulltext Search API](https://europeana.atlassian.net/wiki/spaces/EF/pages/2385739812):

```
https://api.europeana.eu/fulltext/search.json
```

— **not** the general `record/v2/search.json`. Only records whose OCR was ingested
into Europeana's Fulltext API are served there, and only those have IIIF
AnnotationPages, i.e. retrievable page text. (The general Search API's
`text_fulltext=true` flag marks a different, largely disjoint set: records whose
*media file* is text-searchable, whose OCR was never ingested.)

The selection filters are expressed below as API queries. The Search and Entity APIs
require an [API key](https://pro.europeana.eu/page/get-api), sent as an `x-api-key`
header; the IIIF APIs are open.

### Selection filters

**1. Newspapers only**

```
theme=newspaper
```

`theme` is a server-side alias (defined in Europeana's public
[Solr configuration](https://github.com/europeana/search)) that expands to:

```
(proxy_dc_type:("http://data.europeana.eu/concept/18")
 OR edm_datasetName:(*Ag_EU_TEL*Newspapers* OR 9200231* OR 2020128* OR 2020126*
                     OR 15_* OR 92_* OR 124_RoL_BnF_Newspapers OR 18_RoL_ICCU_Foglio))
NOT (foaf_organization:(...19 excluded organisations...))
```

The `proxy_dc_type` branch matches *any* item typed
[concept/18 ("Newspaper")](http://data.europeana.eu/concept/18) regardless of
collection — which is how, for example, a 1989 photograph from a crowdsourcing
campaign can end up in a newspaper query. The build therefore additionally requires
"Newspapers" in `edm:datasetName`. This drops **0 records** in the current corpus;
the counter is kept in `metadata.json` as a tripwire.

**2. Text records only**

```
qf=TYPE:TEXT
```

`TYPE` is a Solr String field carrying the normalized five-value enum (TEXT, IMAGE,
SOUND, VIDEO, 3D) from `edm:type` on the Europeana proxy.

**3. Open reuse rights only**

```
reusability=open
```

An API-level parameter selecting records whose `edm:rights` matches CC0, Public
Domain Mark, CC-BY, or CC-BY-SA (any version). In the current corpus every selected
record carries the Public Domain Mark.

**4. Date-bearing items only**

```
qf=proxy_dcterms_issued:[* TO *]
```

Every item must carry a `dcterms:issued` date. This drops exactly **one record** from
the corpus, and in exchange the whole dataset is partitionable and filterable by year.

> **Trap:** `qf` values on the *same* field are ORed; only different fields are
> ANDed. This filter must therefore never be sent alongside a year range on the same
> field — `proxy_dcterms_issued:[* TO *]` plus `[1873 TO 1874}` means "has a date OR
> is from 1873" and silently matches the entire corpus. The harvest below sends the
> year range *only*.

### Combined query

The corpus definition as a single Search API call:

```
https://api.europeana.eu/fulltext/search.json
  ?query=*
  &theme=newspaper
  &qf=TYPE:TEXT
  &qf=proxy_dcterms_issued:[* TO *]
  &reusability=open
  &profile=rich
  &rows=100
  &cursor=*
```

→ `totalResults`: **995,182**. Two parameters are silently unforgiving:

- **`rows` is capped at 100 server-side.** Asking for 500 returns 100, without
  complaint.
- **`profile=rich` is required.** `dcTypeLangAware`, `dcSubjectLangAware`, `edmPlace`
  and `edmTimespan` appear on no lighter profile, and they are the source of every
  enrichment edge in this dataset.

### Result

| Stage | Records |
|---|---|
| `theme=newspaper` + `TYPE:TEXT` + `reusability=open` (Fulltext Search API) | 995,183 |
| … with a `dcterms:issued` date | 995,182 |
| … skipped as non-newspaper collections | 0 |
| Page sample: issues selected | 1,000 |
| … with retrievable IIIF manifests | 1,000 |
| … whose manifests contain canvases | 889 |
| Pages harvested | 7,001 (6,973 with OCR text) |

## The harvest, step by step

Five request types against three public APIs — roughly 19,000 requests end-to-end.
[`build.py`](https://github.com/storytracer/europeana-open-newspapers) automates all
of it (checkpointed, resumable, HTTP-cached); the endpoints and per-table counts are
recorded in `data/metadata.json`.

**Step 1 — count each publication year** (351 requests: the corpus total plus one
per year, 1600–1949).

The publication date can be *filtered* but is never *returned*: `proxy_dcterms_issued`
is in the index, yet no API profile includes it and `fl` is ignored. Partitioning the
harvest by year is therefore what gives every item its `year_issued` — not a speed
optimization.

```
https://api.europeana.eu/fulltext/search.json
  ?query=*
  &theme=newspaper
  &qf=TYPE:TEXT
  &qf=proxy_dcterms_issued:[1854 TO 1855}
  &reusability=open
  &profile=rich
  &rows=0
```

→ `totalResults: 8499`. The range is half-open (`[1854 TO 1855}`) so adjacent years
can neither overlap nor leave gaps. **Before harvesting anything, assert that the
per-year counts sum exactly to the corpus total** (995,182) — a filter that silently
matches too much or too little fails loudly here, instead of shipping a
plausible-but-wrong dataset.

**Step 2 — drain each year with cursor pagination** (~10,300 requests).

The same query with `rows=100` and `cursor=*`; every response carries a `nextCursor`
to echo back in the next request:

```
https://api.europeana.eu/fulltext/search.json
  ?query=*
  &theme=newspaper
  &qf=TYPE:TEXT
  &qf=proxy_dcterms_issued:[1854 TO 1855}
  &reusability=open
  &profile=rich
  &rows=100
  &cursor=*
```

Two properties of the cursor matter:

- **The chain needs one request more than the maths says.** The API returns a
  `nextCursor` even on the last populated page, so a chain terminates only after a
  further request comes back empty: `ceil(count / 100) + 1` requests per year.
- **Cursor pagination is inherently serial** — each request needs the previous
  response's cursor — so a single chain manages ~1.3 requests/s no matter the rate
  limit. The 350 year chains run concurrently, which makes the harvest
  rate-limit-bound instead of latency-bound.

Each hit becomes one row of `items.parquet`: the LangMap literals
(`dcTitleLangAware`, `dcDescriptionLangAware`, `dcTypeLangAware`), provenance
(`dataProvider`, `provider`, `edmDatasetName`, `rights`), and one enrichment edge per
entity URI found under the `def` key of `dcTypeLangAware` / `dcSubjectLangAware` /
`dcCreatorLangAware` or in `edmConcept` / `edmAgent` / `edmPlace` / `edmTimespan` —
those edges become `enrichments.parquet`. The hit's `id` also yields the two URL
columns: `https://www.europeana.eu/item{id}` (`europeana_url`) and
`https://iiif.europeana.eu/presentation{id}/manifest` (`manifest_url`).

**Step 3 — resolve every linked Europeana entity** (38 requests).

Entity URIs of the form `http://data.europeana.eu/{type}/{id}` resolve via the
Entity API:

```
https://api.europeana.eu/entity/place/216254
```

→ `prefLabel`/`altLabel` in 30+ languages, `broader`/`narrower`, `sameAs` links to
Wikidata, GeoNames and VIAF, coordinates for places, date ranges for timespans —
flattened into `entities.parquet`. (Third-party entity URIs linked directly by
providers are not resolvable here; they keep their URI in `enrichments.parquet`,
plus an English label lifted from the search response where the mapping is
unambiguous.)

**Step 4 — fetch the IIIF Presentation manifest of each sampled issue**
(1,000 requests, no API key needed).

```
https://iiif.europeana.eu/presentation/{collection}/{record}/manifest?format=3
```

`format=3` selects IIIF Presentation v3; the exact URL for every issue is in the
`manifest_url` column. The 1,000 issues are chosen by the stratified sampler
described under "How the page sample was drawn" below — steps 1–3 cover the full
corpus, steps 4–5 only the sample. Each canvas is one physical page, carrying the
page image's URL, dimensions and MIME type, plus — where OCR exists — a reference to
the page's annotation page. The raw manifests ship in `manifests.parquet`.

**Step 5 — fetch each page's annotation page with `profile=text`**
(~7,000 requests, one per page; the API has no batch form).

Take each canvas's `annotations[0].id` and append `profile=text`:

```
https://iiif.europeana.eu/presentation/{collection}/{record}/annopage/{n}?lang={lang}&profile=text
```

Without the profile the annotations merely *reference* the text; with it, the
response inlines everything: the page-granularity annotation carries the full page
text (→ `text` in `pages`), and every block/line/word annotation ties a character
range in that text (`#char=start,end`) to a pixel region on the page image
(`#xywh=x,y,w,h`) (→ `annotations`). This one-request-per-page cost is why full-corpus
text would be ~16.7 million requests and ~950 GB, and pages are sampled instead.

## Dataset structure

Five configurations, one per Parquet table; the default is `pages`.

```python
from datasets import load_dataset

pages = load_dataset("storytracer/europeana-open-newspapers-sample", "pages", split="train")
items = load_dataset("storytracer/europeana-open-newspapers-sample", "items", split="train")
```

### `pages` — OCR page text (sample)

| column | type | description |
| --- | --- | --- |
| `item_id` | string | Europeana item URI, joins to `items` |
| `page_number` | int16 | 1-based position of the page within its issue |
| `page_id` | string | identifier of the IIIF annotation page (null on OCR-less pages) |
| `language` | string | page text language (ISO 639-1; null on OCR-less pages) |
| `text` | string | full OCR text of the page — **null where the page has no OCR** |
| `annotations` | string | JSON array of block/line/word annotations (see below; null on OCR-less pages) |
| `image_url` | string | URL of the page image (IIIF Image API) |
| `image_mime_type` | string | MIME type of the page image |
| `image_width` | int32 | page image width in pixels |
| `image_height` | int32 | page image height in pixels |
| `text_length` | int32 | length of `text` in characters (null on OCR-less pages) |
| `text_rights` | string | rights statement URL for the OCR text of this page (null on OCR-less pages) |

Each element of `annotations` locates a snippet of the page text on the page image:

```json
{"granularity": "word", "text": "Zeitung", "char_start": 1204, "char_end": 1211,
 "bbox_x": 843, "bbox_y": 310, "bbox_w": 164, "bbox_h": 42}
```

`granularity` is `block`, `line` or `word`; `char_start`/`char_end` are offsets into
`text`; the bounding box is in pixels on the page image. Fields are null where the
source annotation lacks them.

**Every physical page of a sampled issue is a row, whether or not it has OCR**: 28 of
the 7,001 pages carry no OCR text upstream and ship as *structural rows* — `text` and
the other OCR-derived columns are null, while `page_number` and the image facts
remain, so an issue's page sequence has no holes and the page images stay reachable
(e.g. for running your own OCR). Filter on `text IS NOT NULL` if you only want text.

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

The `dc_*` columns follow Europeana's LangMap convention — JSON objects keyed by
language tag, values as arrays of strings:

```json
{"de": ["Lienzer Zeitung - 1941-11-15"]}
```

The `enriched_*` columns carry Europeana's semantic enrichment, resolved to English
labels with the source property preserved:

```json
[{"uri": "http://data.europeana.eu/place/216254", "label_en": "Vienna",
  "lat": "48.208199", "lon": "16.3719", "source": "dcterms_spatial"}]
```

**Every issue has an exact publication date**, obtained twice over: `year_issued`
comes from Europeana's date index (via the harvest partition — the API never returns
the date field itself), and `date_issued` is parsed from the issue title, which ends
in the date in all 11 collections. The two agree for 995,181 of 995,182 issues; the
single disagreement (across a New Year boundary) is kept as harvested.

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
| `language` | string | label language; null for non-label facts |

The `sameAs`/`exactMatch` rows link Europeana entities to Wikidata, GeoNames, VIAF
and other authority files.

### `manifests` — raw IIIF manifests (sample)

| column | type | description |
| --- | --- | --- |
| `item_id` | string | joins to `items` and `pages` |
| `manifest_url` | string | where the manifest was fetched from |
| `manifest` | string | the complete IIIF Presentation (v3) manifest as JSON |

Included as provenance: the manifests are the exact source documents the `pages` rows
were derived from — including, for the 111 sampled issues without any pages, the
canvasless manifests that explain their absence.

## How the tables relate

```
items.parquet                       One row per issue, complete corpus
    │
    │  item_id
    │
    ├── pages.parquet               Page text + annotations (sampled issues)
    │
    ├── manifests.parquet           Raw IIIF source of those pages
    │
    └── enrichments.parquet         Which entity was linked via which property
            │
            │  entity_uri
            │
            └── entities.parquet    Labels, hierarchies, authority links, coordinates
```

## Example queries

All examples use [DuckDB](https://duckdb.org/) and query the hosted files directly.

```sql
-- Page text of the sample (skip the 28 structural rows)
CREATE TABLE pages AS SELECT * EXCLUDE (annotations)
FROM 'hf://datasets/storytracer/europeana-open-newspapers-sample/data/pages/*.parquet'
WHERE text IS NOT NULL;

-- Issues per decade, whole corpus
SELECT (year_issued // 10) * 10 AS decade, COUNT(*) AS issues
FROM 'hf://datasets/storytracer/europeana-open-newspapers-sample/data/items.parquet'
GROUP BY decade ORDER BY decade;

-- Front pages, joined to their issue metadata
SELECT p.text, i.dc_title, i.date_issued, i.data_provider
FROM pages p
JOIN 'hf://datasets/storytracer/europeana-open-newspapers-sample/data/items.parquet' i
  ON p.item_id = i.item_id
WHERE p.page_number = 1
LIMIT 10;

-- The most multilingual issues: pages whose language differs from the issue's
SELECT p.item_id, i.language AS issue_lang, p.language AS page_lang
FROM pages p
JOIN 'hf://datasets/storytracer/europeana-open-newspapers-sample/data/items.parquet' i
  ON p.item_id = i.item_id
WHERE p.language <> i.language;

-- Entities: Wikidata links for the places items are enriched with
SELECT DISTINCT e.entity_uri, e.value AS authority_uri
FROM 'hf://datasets/storytracer/europeana-open-newspapers-sample/data/entities.parquet' e
WHERE e.entity_class = 'edm_Place' AND e.field = 'sameAs'
  AND e.value LIKE '%wikidata%';
```

Word-level annotations are JSON per page; in Python:

```python
import json

page = pages[0]
words = [a for a in json.loads(page["annotations"]) if a["granularity"] == "word"]
# each word: text, char_start/char_end into page["text"], bbox_* in image pixels
```

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
newspapers** — a coverage that holds even among the issues with page text:

| collection | corpus share | sampled | | collection | corpus share | sampled |
|---|---|---|---|---|---|---|
| Netherlands | 32.7% | 325 | | Estonia | 2.8% | 29 |
| Austria (ONB) | 14.8% | 148 | | Finland | 2.4% | 25 |
| Hamburg | 13.2% | 131 | | Belgrade | 2.2% | 23 |
| Berlin | 11.7% | 117 | | Poland | 1.5% | 16 |
| Tessmann | 11.5% | 115 | | Luxembourg | 0.1% | 2 |
| Latvia | 6.9% | 69 | | | | |

889 of the 1,000 sampled issues have retrievable pages (7,001 rows, 6,973 of them
with OCR text); the remaining 111 have IIIF manifests with **no canvases at all**, an
upstream gap that is strongly concentrated in one collection — 104 of the 111 are
Austrian National Library issues, mostly from the 1850s–1870s. Every skipped record
is accounted for in the build's error log, and the canvasless manifests themselves
are preserved in `manifests.parquet`.

**Caveat for statistical use:** this is a *coverage* sample, not a probability
sample. The decade floor and the title round-robin deliberately over-represent rare
decades and small newspapers; corpus-level statistics computed from `pages` should be
weighted accordingly (or computed on `items`, which is complete).

## Dataset composition

### Collections

| collection (edm:datasetName) | institution | issues |
|---|---|---|
| `9200359_…_Newspapers_Netherlands` | National Library of the Netherlands | 325,656 |
| `9200300_…_Newspapers_ONB` | Austrian National Library | 147,518 |
| `9200338_…_Newspapers_HamburgLibrary` | State and University Library Hamburg | 131,076 |
| `9200355_…_Newspapers_Berlin` | Berlin State Library | 116,456 |
| `9200333_Newspapers_TessmannLibrary` | Teßmann Library (South Tyrol) | 114,771 |
| `9200303_…_Newspapers_Latvia` | National Library of Latvia | 68,661 |
| `9200356_…_Newspapers_Estonia` | National Library of Estonia | 28,128 |
| `9200301_…_Newspapers_Finland` | National Library of Finland | 24,196 |
| `9200339_…_Newspapers_Belgrade` | University of Belgrade | 22,273 |
| `9200357_…_Newspapers_poland` | National Library of Poland | 15,130 |
| `9200396_…_Newspapers_Luxembourg` | National Library of Luxembourg | 1,317 |

### Languages

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

The table above is issue-level. Page-level text is more multilingual than the issue
metadata suggests: the sampled pages additionally include Russian, Swedish, Italian,
English, Spanish and Hebrew pages (multilingual papers in the Baltic, Finnish and
Austrian collections), with each page's own language in the `language` column of
`pages`.

### Text volume

The 6,973 text pages carry **~98 million characters** of OCR (median 12,538 per page)
and ~1.9 GB of word-level annotations (uncompressed) — the annotations, not the text,
are ~90% of the `pages` files' size.

## From IIIF and EDM to Parquet: design decisions

Choices made when flattening Europeana's APIs into tables, documented as a replicable
pattern for further Europeana full-text datasets.

### Two layers per enrichable field

`items` carries the provider's literals (`dc_title`, `dc_type` as LangMaps) *and*
Europeana's enrichment (`enriched_*`, `dc_*_en`) side by side, so the value the
library wrote and the entity Europeana resolved it to are both visible in every row —
and the edge-level provenance survives in `enrichments.parquet`.

### The publication date is obtained twice, deliberately

Europeana's API can *filter* on `proxy_dcterms_issued` but never *returns* it, on any
profile. `year_issued` is therefore derived from the year-partitioned harvest itself,
and `date_issued` is parsed from the issue titles (which end in the date in all 11
collections). Two independent routes to the same fact, agreeing on 995,181 of 995,182
issues, each verifiable against the other.

### Collections are keyed on `dataset_name`, not `data_provider`

`data_provider` is a free-text label with spelling variants — the Austrian National
Library appears under two names ("Austrian National Library", 139,674 issues, and
"Österreichische Nationalbibliothek - Austrian National Library", 7,844) for a single
collection, and would draw a double quota in sampling. `edm:datasetName` comes from
the ingest pipeline and is stable.

### Pages without OCR are rows, not gaps

A canvas without OCR still physically exists, and its image is still reachable. It
ships as a structural row (null `text`, intact `page_number` and image facts) rather
than a silent hole in the page sequence; `pages` − `pages_with_text` in
`metadata.json` counts them.

### Raw manifests ship alongside the derived tables

`manifests.parquet` preserves the exact IIIF source of every sampled issue at zero
additional harvest cost — the derivation from source to table is auditable row by
row, including for the issues that yielded no pages.

### Determinism over convenience

Issue selection hashes item ids with md5 (not Python's salted `hash()`), strata are
iterated in sorted order, and the API's per-year counts are reconciled against the
corpus total before anything is written. The result: the dataset is exactly
reproducible from the public APIs with the published build script.

## Provenance and reproducibility

The dataset is built by a single self-contained script
([`build.py`](https://github.com/storytracer/europeana-open-newspapers), Python ≥ 3.11
with [uv](https://docs.astral.sh/uv/)): one command reproduces it end-to-end,
including the identical sample. The harvest is checkpointed, resumable and
HTTP-cached; `data/metadata.json` records the harvest date, the exact endpoints and
per-table counts, including the tripwire counters (`skipped_non_newspaper_datasets`,
`pages` vs `pages_with_text`).

## Limitations

- **OCR quality varies** with the age and condition of the source material and is not
  manually corrected. Treat as historical OCR, not ground truth.
- **Page text is a sample** — 1,000 issues of 995,182. See the sampling caveat above;
  `items` is complete.
- **111 sampled issues have no pages** (canvasless IIIF manifests upstream, 104 of
  them ONB, 1850s–1870s), and 28 pages ship without OCR text.
- **Dates are as published by the libraries**: `date_issued` is parsed from the issue
  title; one issue in 995,182 disagrees with the date index and is kept as harvested.
- **Coverage reflects Europeana's Fulltext API ingest**, not European newspaper
  history: 14 page-level languages and 9 countries, with German and Dutch dominant.

## Licensing

- **Metadata** (items, enrichments, entities, manifests):
  [CC0](https://creativecommons.org/publicdomain/zero/1.0/), per Europeana's Data
  Exchange Agreement.
- **Content** (page text and images): the harvest selects `reusability=open`
  (CC0, Public Domain Mark, CC-BY, CC-BY-SA); in the current corpus, **every issue and
  every page carries the
  [Public Domain Mark](http://creativecommons.org/publicdomain/mark/1.0/)**. The
  per-record statement is on every row regardless — `image_rights` in `items`,
  `text_rights` in `pages` — so the licensing remains verifiable if the corpus
  changes.

When reusing content, credit the providing institution identified by `data_provider`.

## Acknowledgements

The newspapers were digitised, OCRed and openly licensed by the contributing
institutions: the National Library of the Netherlands, the Austrian National Library,
the State and University Library Hamburg, the Berlin State Library, the Teßmann
Library, the National Library of Latvia, the National Library of Estonia, the
National Library of Finland, the University of Belgrade, the National Library of
Poland and the National Library of Luxembourg — aggregated and enriched by
[Europeana](https://www.europeana.eu/).
