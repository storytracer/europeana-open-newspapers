# CLAUDE.md

Guidance for working in this repo. See [README.md](README.md) for what the dataset is
and how to run the build.

## Shape of the project

One script, [build.py](build.py) — a self-contained `uv` script (dependencies live in
the PEP 723 header, not a lockfile). There is no test suite and no package layout. Run
it with `./build.py`; the shebang handles dependencies.

Three phases, each depending on the last: **items → entities → pages**. Each writes
part-files into `data/output/parts/` and merges them into the final Parquet at the end
of the phase. Progress lives in `checkpoint.json`; a phase marked `finalized` is skipped
on re-run.

## Testing changes

Never test against the full corpus (~10k requests). Two hidden env hooks scope a run
down, and the HTTP cache makes repeat runs nearly instant:

```bash
rm -rf data/output   # a finalized checkpoint makes phases skip themselves
EOT_MAX_PARTITIONS=2 EOT_MAX_REQUESTS=2 ./build.py --max-items 2
```

`EOT_MAX_PARTITIONS` caps the number of year partitions; `EOT_MAX_REQUESTS` caps the
cursor requests per partition. Test runs go in the real `data/output`, not a scratch dir.

After a schema change, delete `data/output` before re-running — the merge step will
otherwise fail against part-files with the old schema.

## Europeana API traps

These are all verified against the live API, and several of them fail *silently* — they
produce a plausible dataset that is quietly wrong. Do not take a filter's word for it;
check its `totalResults` against a known number.

- **`qf` values on the same field are ORed** (different fields are ANDed). Sending
  `proxy_dcterms_issued:[* TO *]` alongside a year range means "has a date OR is from
  1873", which matches the entire corpus. Partition queries send the year range only.
- **`europeana_id` is not usable as a filter.** It is not indexed as a string, so
  `europeana_id:/9200359/*` silently matches *everything* rather than that dataset.
  It does not error.
- **The publication date can be filtered but never read.** `proxy_dcterms_issued` is
  searchable, but no profile returns it — not `rich`, not any of the other eight, and
  `fl` is ignored. This is why `year_issued` is derived from the harvest partition an
  item came out of rather than from the payload.
- **`YEAR` / `edm_year` are effectively empty** in this index (2 items out of 995k), and
  `edmTimespan` is null for real newspaper items. Do not reach for them as a date source.
- **`rows` is capped at 100** server-side. Asking for 500 returns 100, without complaint.
- **`rich` is required.** `dcTypeLangAware`, `dcSubjectLangAware`, `edmPlace` and
  `edmTimespan` appear on no lighter profile, and they are the source of every
  enrichment edge.
- **The Fulltext Search API is not `record/v2/search.json`.** Only records ingested into
  the Fulltext API are served here, and only those have IIIF AnnotationPages (i.e. page
  text). The Search API's `text_fulltext=true` flag is a different, largely disjoint set.

## Design decisions worth preserving

**Year partitions.** The items phase runs one cursor chain per publication year,
concurrently. This is not just for speed: cursor pagination is inherently serial (each
request needs the previous response's `nextCursor`), so a single chain is capped at
~1.3 req/s no matter what `--rate-limit` says. Partitioning makes the rate limit the
binding constraint — and it is what gives each item its `year_issued`, which the API
otherwise refuses to hand over. `discover_year_partitions` asserts the per-year counts
sum to `totalResults` before harvesting starts; keep that assertion, it is the guard
against the silent-mismatch failure mode above.

**No all-null columns.** Three columns have been removed after turning out to be
unfillable: `image_byte_size` (Europeana's IIIF image server derives JPEGs on the fly,
sends no `Content-Length` and ignores `Range`), `image_status` (with it, the probe phase
that populated it went too), and `edm_year` (the field is never returned). If a column
cannot be populated, delete it rather than shipping nulls.

**Silent drops get counted.** Items skipped as non-newspaper are counted into
`sample_metadata.json`, not just dropped. A number that jumps unexpectedly is the signal
that a filter has started catching more than intended.

**Pages is a sample, deliberately.** Up to `--max-items` per *dataset* — grouped by
`dataset_name`, not `data_provider`. `data_provider` is a free-text label with spelling
variants (the Austrian National Library appears under two names for one dataset, and
would get a double quota); `dataset_name` is a controlled identifier from the ingest
pipeline.
