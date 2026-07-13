# CLAUDE.md

Guidance for working in this repo. See [README.md](README.md) for what the dataset is
and how to run the build.

## Shape of the project

One script, [build.py](build.py) — a self-contained `uv` script (dependencies live in
the PEP 723 header, not a lockfile). There is no test suite and no package layout. Run
it with `./build.py`; the shebang handles dependencies.

Internally the script is a set of classes: `Settings` (all tunables, frozen), `Schemas`
(the four output Parquet schemas — the compatibility contract), `EuropeanaApi` (cached +
rate-limited + retrying HTTP), `Checkpoint`/`ParquetStore`/`ErrorLog`/`MetadataFile`
(state and I/O), pure parsers (`ItemExtractor`, `EntityParser`, `PageParser`,
`TitleParser`, `EntityRef`), `Sampler`, and one `Phase` subclass per phase under a
`BuildPipeline`. Every fan-out runs through the same worker/queue machinery
(`Phase._run_pipeline`): jobs drain through a `WorkerPool`, handlers emit result
messages into a bounded queue, and a single writer coroutine per phase owns all disk
I/O — so part writes and checkpoint saves never interleave, and one failed job never
cancels its siblings.

Three phases, each depending on the last: **items → entities → pages**. Each writes
part-files into `data/output/parts/` and merges them into the final Parquet at the end
of the phase. Progress lives in `checkpoint.json`; a phase marked `finalized` is skipped
on re-run.

## Testing changes

Never test against the full corpus (~10k requests). Two hidden env hooks scope a run
down, and the HTTP cache makes repeat runs nearly instant:

```bash
rm -rf data/output   # a finalized checkpoint makes phases skip themselves
EOT_MAX_PARTITIONS=2 EOT_MAX_REQUESTS=2 ./build.py --sample-size 2
```

`EOT_MAX_PARTITIONS` caps the number of year partitions; `EOT_MAX_REQUESTS` caps the
cursor requests per partition. Test runs go in the real `data/output`, not a scratch dir.

After a schema change, delete `data/output` before re-running — the merge step will
otherwise fail against part-files with the old schema. A rebuild is cheap: every response
is cached and never expires, so a full re-run replays from disk in a couple of minutes
rather than re-hitting the API.

`Sampler` is pure and reads only `items.parquet`, so sampling changes can be checked
against the real 995k-row corpus without any network at all — import it and inspect the
distribution rather than running the pages phase.

## Europeana API traps

These are all verified against the live API, and several of them fail *silently* — they
produce a plausible dataset that is quietly wrong. Do not take a filter's word for it;
check its `totalResults` against a known number.

- **`qf` values on the same field are ORed** (different fields are ANDed). Sending
  `proxy_dcterms_issued:[* TO *]` alongside a year range means "has a date OR is from
  1873", which matches the entire corpus. Partition queries send the year range only.
- **Slashes in a `qf` value are regex delimiters.** `europeana_id:/9200359/*` is parsed
  as a *regex* and silently matches the entire corpus. Escaped —
  `europeana_id:\/9200359\/*` — it correctly returns just that dataset (325,656).
  The field is `string`/`indexed`/`docValues` and perfectly usable; the escaping was the
  bug. Quoting it instead (`"/9200359/*"`) matches nothing.
- **The publication date can be filtered but never read.** `proxy_dcterms_issued` is
  `stored="true"` in the Solr schema, so the index has it — but no API profile returns
  it (not `rich`, not any of the other eight) and `fl` is ignored. It is the API layer
  that withholds it, not the index. This is why `year_issued` is derived from the
  harvest partition, and `date_issued` is parsed out of `dc_title`.
- **`YEAR` / `edm_year` are effectively empty** in this index (2 items out of 995k), and
  `edmTimespan` is null for real newspaper items. Do not reach for them as a date source.
  (`YEAR` is a `copyField` of `proxy_edm_year`, which these records never populate.)
- **`rows` is capped at 100** server-side. Asking for 500 returns 100, without complaint.
- **`rich` is required.** `dcTypeLangAware`, `dcSubjectLangAware`, `edmPlace` and
  `edmTimespan` appear on no lighter profile, and they are the source of every
  enrichment edge.
- **The Fulltext Search API is not `record/v2/search.json`.** Only records ingested into
  the Fulltext API are served here, and only those have IIIF AnnotationPages (i.e. page
  text). The Search API's `text_fulltext=true` flag is a different, largely disjoint set.
- **A cursor chain needs one request more than the maths says.** The API returns a
  `nextCursor` even on the last populated page, so a chain terminates only after a
  further request that comes back empty: `ceil(count / 100) + 1`, not `ceil(count / 100)`.
- **Europeana 502s under load, sometimes for minutes.** A cursor chain cannot skip a page,
  so giving up costs the whole partition. `RetryPolicy` spans ~4 minutes of capped
  exponential backoff, with jitter so that 300+ concurrent chains hit by the same outage
  do not retry in lockstep and re-spike the server as it recovers.

## The index behind the API

Europeana publish their Solr config: [europeana/search](https://github.com/europeana/search),
`solr_confs/fulltext/conf/` — that is the index serving the Fulltext Search API. (Do not
read `solr_confs/newspapers/`; it is the legacy TEL config and does not describe this
API.) `schema.xml` is the ground truth for what is filterable, and `query_aliases.xml`
defines the `theme=` pseudo-field.

**`theme=newspaper` resolves to this**, which is the real definition of our corpus:

```
(proxy_dc_type:("http://data.europeana.eu/concept/18")
 OR edm_datasetName:(*Ag_EU_TEL*Newspapers* OR 9200231* OR 2020128* OR 2020126*
                     OR 15_* OR 92_* OR 124_RoL_BnF_Newspapers OR 18_RoL_ICCU_Foglio))
NOT (foaf_organization:(... 19 excluded orgs ...))
```

Two things follow. The `proxy_dc_type` branch matches *any* item typed "Newspaper"
regardless of collection — that is how a 1989 photo from a crowdsourcing campaign ends
up in a newspaper query. And Europeana's own list of newspaper datasets includes names
with no "Newspapers" in them (`18_RoL_ICCU_Foglio`, `15_*`, `92_*`), which our
`Settings.newspaper_dataset_substring` filter would drop. It drops nothing today (the counter in
`sample_metadata.json` is 0), but that counter is the tripwire: if it ever goes up,
check the dropped names against the alias above before assuming the filter is right.

**There is a real date field, `issued`** (`type="date"`, docValues), distinct from the
string `proxy_dcterms_issued` the partitions use. Both give identical counts (1854:
8,499 items either way; 995,182 date-bearing items either way), so the year partitioning
is confirmed against an independent field. Either works as a partition key; `issued`
takes proper datetime bounds (`issued:[1854-01-01T00:00:00Z TO 1855-01-01T00:00:00Z}`).

## Design decisions worth preserving

**Year partitions.** The items phase runs one cursor chain per publication year,
concurrently. This is not just for speed: cursor pagination is inherently serial (each
request needs the previous response's `nextCursor`), so a single chain is capped at
~1.3 req/s no matter what `--rate-limit` says. Partitioning makes the rate limit the
binding constraint — and it is what gives each item its `year_issued`, which the API
otherwise refuses to hand over. `ItemsPhase._discover_partitions` asserts the per-year
counts sum to `totalResults` before harvesting starts; keep that assertion, it is the
guard against the silent-mismatch failure mode above.

**Nothing is finalized until it is complete.** `ItemsPhase._finalize` sets `finalized`,
and a finalized phase is skipped forever after. `ItemsPhase._harvest` therefore refuses
to finalize unless every partition reports `done`, and raises instead. Without that
guard a partition that failed would merge the years that happened to succeed, declare
the phase complete, and ship a dataset with entire decades missing — with no error
anywhere.

**Progress counts partitions, not requests.** A resumed cursor chain restarts from its
last flushed cursor and re-issues everything since (from cache), so a per-request counter
double-counts on every resume and eventually creeps past its total — at which point tqdm
silently drops the bar and renders exactly as if `total=None`. Partitions complete once.
For the same reason a partition's `requests` counter is only persisted at flush, in
lockstep with the cursor (both travel in one `ItemsFlush` message and land in one atomic
checkpoint save): advancing it per-request leaves it ahead of the cursor after a crash.

**No all-null columns.** Four columns have been removed after turning out to be
unfillable: `image_byte_size` (Europeana's IIIF image server derives JPEGs on the fly,
sends no `Content-Length` and ignores `Range`), `image_status` (with it, the probe phase
that populated it went too), `edm_year` (never returned), and `parent_id` (the Fulltext
Search API does not return `dctermsIsPartOf`, so it was null for all 995k rows). If a
column cannot be populated, delete it rather than shipping nulls.

**Silent drops get counted.** Items skipped as non-newspaper are counted into
`sample_metadata.json`; issues whose manifest has no canvases are written to
`errors.log`. Both used to vanish without trace. A number that jumps unexpectedly is the
signal that a filter has started catching more than intended.

**Pages is a sample, deliberately** — the full corpus is ~15.7M pages, ~16.7M requests
(one per page; the IIIF annotation API has no batch form) and ~950 GB, of which 90% is
the word-level `annotations` column, not the text.

`Sampler` stratifies four levels deep, and each level exists because of a specific way
the corpus is lopsided:

- **dataset** — keyed on `dataset_name`, not `data_provider`. `data_provider` is
  free-text with spelling variants (the Austrian National Library appears under two names
  for one dataset, and would draw a double quota); `dataset_name` comes from the ingest
  pipeline. `--sample-strategy` picks proportional (`Sampler.allocate`) or equal
  (`Sampler.allocate_equal`) shares across them.
- **decade** — proportional within the dataset, with a floor of 1. Without the floor a
  proportional split rounds whole decades to zero; the 1820s are 3.7% of the corpus and
  were missing entirely from the old sample.
- **title** — round-robin. `TitleParser.newspaper_title` has to strip both the trailing
  date *and* issue numbering (`, nr: 16`), or Finland's 24k issues look like 3,572
  separate newspapers and the round-robin does nothing.
- **issue** — ordered by `md5(item_id)`. Item ids sort by title then date, so taking the
  first N returned 25 consecutive issues of a single newspaper. It must be md5 rather
  than `hash()`, which is salted per process and would make the sample unreproducible.

The old strategy covered 20 of 34 decades with 47 titles; this one covers all 34 with
~300. Verify a change by importing `Sampler` and comparing decade shares against the
corpus — not by eyeballing the sample size.
