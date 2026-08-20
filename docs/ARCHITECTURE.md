# Architecture

TileDistillery is a reusable GitHub Actions pipeline that turns raw
[Geofabrik](https://download.geofabrik.de) OSM extracts into finished
PMTiles vector-tile layers, using [tilemaker](https://github.com/systemed/tilemaker)
(see "Tile-build engine" below) against a config a *caller* repo supplies.
Unlike [TileAlchemist](https://github.com/foxandfeature/tilealchemist) (a
sibling project solving an adjacent problem, not a dependency of this one),
there is no profile system here: a caller points the pipeline at its own
tilemaker JSON config + Lua process script, the same way TileAlchemist
callers point at a Python profile file. TileDistillery itself never ships a
domain-specific config; only a minimal dummy one for CI self-tests (see
"CI self-test").

TileAlchemist's `docs/ARCHITECTURE.md` describes a similar-shaped problem:
spreading work across many GitHub Actions runners, merging shards, publishing
to two targets. Several of its patterns reappear below (reusable
pipeline workflow separated from publishing; small isolated worker jobs to
respect the 6-hour job limit; a cross-repo-safe Releases path plus a
repo-guarded B2 path). Its actual sharding mechanism does not: TileAlchemist
splits a single PMTiles archive into equal contiguous byte ranges by offset,
which only works because a PMTiles directory gives you exact tile
offsets/lengths up front. There is no equivalent here, since a Geofabrik OSM
extract has no analogous fixed-size index, so distribution is a claim-based
queue over heterogeneously-sized regions instead (see below), not a
byte-range partition.

## Region detection

`scripts/leaves.py` fetches Geofabrik's `index-v1.json`, builds the
parent/child tree, and takes every node with no children as one shard
("leaf"): the smallest extract Geofabrik publishes along that branch (e.g.
`europe/germany/bavaria`, not all of `europe/germany`). This is the
granularity of a work item everywhere below: "region" always means "one
leaf." Leaves are not merged or split any further; an oversized leaf (Russia)
and a tiny one (Vatican) are both single, indivisible units of work (see
"Timing history" for how that asymmetry is handled).

`find_leaves()` also drops a hardcoded `KNOWN_REDUNDANT_LEAVES` set, for
three distinct reasons `index-v1.json` gives no flag for. See the comment
above that set in `leaves.py` for the full reasoning and sources:

- the US Census regions (declared parent `north-america` instead of `us`,
  so nothing marks them as sitting on top of the individual `us/<state>`
  extracts);
- Geofabrik's own explicitly labeled "Special Sub Regions" (`dach`, `alps`,
  ...), which would otherwise get claimed and built as a "leaf" in its own
  right even though, e.g., `germany`, `austria`, and `switzerland` already
  cover the same ground as `dach`;
- `enfield`, the one London borough Geofabrik happens to publish as its own
  extract while every other borough is only available inside
  `greater-london` itself. Left in, the general "a referenced parent is
  coarser, drop it" rule would exclude `greater-london` for having a child,
  and the run would fetch tiny Enfield while silently never fetching the
  rest of London at all. `find_leaves()` has to special-case this one the
  other way: a redundant leaf must not keep shadowing its own parent.

## Distribution: dynamic claim queue, not a static matrix

A static `strategy: matrix` over ~hundreds of leaves would technically run
to completion, but gives up exactly the property this pipeline needs: which
matrix cell GitHub Actions starts next, as slots free up, isn't a documented
contract, so there's no reliable way to make it start the longest-running
regions first. It would also pay full job startup cost (checkout, tilemaker
binary, Geofabrik download setup) once per *leaf*, which is wasteful when
most leaves are small and there are hundreds of them.

Instead, `_pipeline.yml` starts a fixed number of long-lived **worker**
jobs in one `build` matrix (`worker_count` input, default ~20, since GitHub
already queues more than ~20 concurrently *running* jobs on a public repo,
so this is comfortably real parallelism, not queued waiting; see
TileAlchemist's ARCHITECTURE.md "Parallelism" for the same reasoning). Each
worker runs a loop, not a single region:

1. Claim the best remaining candidate from the precomputed, duration-sorted
   list (see "Timing history"), one claim per region, never per
   `output_basename` (see "Locking" for why that matters when a call builds
   more than one layer together). The tilemaker image is pulled
   unconditionally, before this first claim even happens (see
   `claim-and-build`'s step order): a worker that ends up with nothing to
   claim has wasted that several-hundred-MB download, but `worker_count` is
   never set higher than the queue needs, so that's rare, and not worth the
   extra branching to avoid. If nothing is claimable, the worker exits
   immediately.
2. Download that leaf's `.osm.pbf` **once**, run tilemaker once per
   `output_basename` in this call against those same bytes (see "Multiple
   layers, one download" below), upload each resulting
   `<region-id>--<output_basename>.mbtiles`, record the claim as done and
   append this run's duration to the timing history.
3. Loop back to 1. Exit when no claimable candidate remains, or the job's
   own time budget runs low.

This is the literal implementation of "a worker requests the next region as
soon as it's done": the next claim happens inside the same job, immediately,
with the tilemaker binary and checkout already warm, not via a fresh job
dispatch per region.

There is deliberately no second, top-up claim-loop round (see "Worker job
shape & the 6-hour limit"): `strategy: fail-fast: false` on the `build`
matrix means a failing leg never cancels its siblings, so the rest of the
fleet just keeps draining the same shared queue regardless of how any one
worker ends, kept simple on purpose, one round, one worker_count, no
conditional second dispatch to reason about.

## State lives in the caller's repo, not this one

The state branch (claims + timing history, see below) lives on a `state`
branch **in the calling repository**, not centrally in
`foxandfeature/tiledistillery`. This isn't just tidiness: the automatic
`secrets.GITHUB_TOKEN` a workflow run gets is scoped to the repository the
workflow is running in, with no cross-repo write access, so a third-party
caller's token could never write refs/contents to *this* repo even if the
architecture wanted it to. Keeping state in the caller's own repo means the
caller's own token, with a `permissions: contents: write` grant on its own
calling workflow (documented as part of the "Consumer contract" below),
is exactly what's needed and nothing more; `_pipeline.yml`'s jobs never
touch any repository but the one that called them. The `prepare` job
creates the `state` branch on first use if it doesn't exist yet (an empty
orphan commit + `refs/heads/state`), so a caller never has to set it up by
hand.

One consequence: state is no longer shared *across* different callers by
construction (they're different repos entirely), but a single repo can
still call the pipeline for more than one `output_basename` (e.g. two
different layers), so claims and timing history are still scoped by
`output_basename` within that repo's own branch (see below).

## Locking: one shared JSON queue file, claimed in small batches

Up to `worker_count` workers can race to claim the same region. Each build
(within this repo) has its own scope key identifying which one is running:
`state/queue/<output_basename>.json`, e.g. a `bins` layer build claims
against `state/queue/bins.json`, and a call building `bins` and `roads`
together (see "Multiple layers, one download" below) claims against
`state/queue/bins,roads.json`. The raw input value, comma(s) and all, is
the scope; it isn't split apart or deduplicated against a separate
`bins`-only build's own scope, since those really are different builds with
independent claim/timing history. This one file *is* the queue: a
`remaining` list (every leaf still up for grabs, `{"id", "pbf_url"}` each,
longest-first: see "Timing history & queue ordering" for how that order is
computed) plus three plain region-id lists: `lock`, `done`, `failed`. The
`prepare` job seeds it once at the start of the run (`leaves.py`'s
`write_queue`); from there, claiming a region means popping it out of
`remaining` and into `lock`.

**This replaced two earlier designs**, in order:

1. One git ref per region (atomic `POST /repos/.../git/refs`, one ref under
   `refs/claims/<scope>/<region-id>/...`, relying on its 422-on-exists for
   compare-and-swap). Real atomicity, but *maximally chatty*: `claim.py
   next` re-listed the whole scope's refs on every single region grabbed,
   and `done`/`failed` each cost their own create+delete. Across
   `worker_count` workers times "hundreds of leaves" (see "Distribution"),
   a full run made thousands of GitHub API calls, routinely exhausting the
   calling workflow's GITHUB_TOKEN's primary hourly quota (GitHub's "API
   rate limit exceeded for installation", distinct from, and much slower
   to clear than, the secondary/abuse-detection limit `common.py`'s
   retry/backoff is tuned for). Git refs also have no bulk-create endpoint,
   so that per-region request cost was structural, not a tuning problem.
2. A JSON claims file claimed in batches (`state/claims/<scope>.json`,
   holding only `lock`/`done`/`failed`), read against a *separate*,
   read-only `queue-manifest.json` (the sorted candidate list), built once
   by `prepare` and passed to every job as a workflow artifact. This fixed
   the request-volume problem (batching, below), but kept the sorted
   candidate list and the claim state as two independently-fetched sources
   of truth that every consumer (`claim.py`, `finalize_check.py`) had to
   cross-reference by hand, and needed an upload/download roundtrip through
   3 separate jobs for data that never actually changed during the run.
   Merging the two removes that roundtrip and the cross-referencing: a
   region's presence in `remaining` already *is* "still claimable", with no
   second file to check it against.

Region ids go straight into these lists as plain strings, with no
git-ref-safe-character stripping. That sanitization existed only because
the first design above embedded region ids literally into git ref names;
nothing here does that anymore, so `scripts/common.py`'s
`sanitize_ref_component` is unused for this purpose (it's still used for
`ensure_branch`'s own ref handling, unrelated to the queue). Two fields
travel with each `remaining` entry, not one, even though a region id alone
would be a unique key: `pbf_url` isn't derivable from `id` (Geofabrik's own
`urls.pbf` field is fetched from `index-v1.json`, not constructed (see
`leaves.py`'s module docstring for a concrete counterexample: `us/wisconsin`
corrects Geofabrik's `parent: north-america` via `effective_parent` so
`--region-scope us` can prefix-match every state, but the *URL* path is
still `north-america/us/wisconsin`, since Geofabrik's own site structure
doesn't get that correction). Keeping `id` as the real key (not a URL
fragment) is also what keeps `state/timings/<output_basename>.json`'s keys
stable independent of Geofabrik's own URL/CDN choices.

The file *can* batch: claiming (`claim.py`'s `_next_region`) takes up to
`--batch-size` (default 5) regions in a single `update_json_file_with_retry`
read-modify-write, caching the rest in memory for the rest of that
worker's `run-worker` loop, so a worker only touches the shared file once
every `--batch-size` regions instead of once per region.
`update_json_file_with_retry` already retries the whole read-modify-write
on a conflicting concurrent write from another worker, re-picking a fresh
batch against the freshly-read `remaining` each attempt, so two workers
racing this can't end up with an overlapping batch: the same correctness
guarantee the original ref-per-region compare-and-swap gave, just amortized
over a batch instead of paid per region.

`done`/`failed` don't touch the shared file at all, to avoid turning it
into the exact hot lock `state/timings/...json` already had to avoid (see
"Timing history" below): with claiming *and* completion both landing on
the same file, that risk would double. Instead they append an outcome line
to the same local per-worker buffer file timing durations already used
(`--timings-buffer`, now carrying a `status` per line too), and
`flush-timings`, run once by a single dedicated job after `build`
finishes, merges every worker's buffer into the shared file's `done`/
`failed` lists (moving the matching ids out of `lock`) in one
read-modify-write for the whole run. See `cmd_flush_timings`'s docstring in
`scripts/claim.py`.

There is deliberately no staleness sweep or crash-reclaim: a `lock` entry
is only ever released by `flush-timings` processing the worker's buffered
outcome for it. A worker that never gets there (job cancelled, OOM-killed,
or hits the time limit mid-build, or whose buffer artifact never uploads)
leaves its claimed batch's remaining region(s) (up to `--batch-size` of
them) stuck for the rest of the run: there is no second round to pick
them up (see "Worker job shape & the 6-hour limit"), and no amount of extra
`worker_count` reclaims them either, since every other worker still treats
an existing `lock` entry as unavailable regardless of who (or what) is
behind it. This is the one real cost of batching over the original
one-ref-per-region design (a crash strands up to `--batch-size` regions
instead of exactly one), kept small by keeping `--batch-size` small, and
accepted for the same reason the original design accepted it:
`cleanup_claims.py` resets the whole scope's file at the end of the run
regardless of how it went (see below), so a stuck lock can't outlive the
run it happened in, and the practical cost is that the *specific regions*
affected need a manual rerun (e.g. via `region_scope`) rather than the
whole run self-healing through a second pass. A lease/timestamp per lock
entry, enabling reclaim *within* a still-running run rather than only at
the next run, is an obvious extension if stuck batches turn out to be a
real, recurring problem in practice, deliberately not built preemptively
(see "Deliberately deferred").

Why a shared file at all, when the original design's docs used to argue
against one: that argument was about *per-claim* writes. A naive "every
claim is its own read-modify-write" design does need the same
retry-on-conflict handling refs avoided, without refs' create-if-absent
guarantee. Batching doesn't remove that per-write contention risk, it just
amortizes it over `--batch-size` regions per write instead of one: the
same tradeoff `state/timings/...json` already made (see below) for exactly
this reason. What actually changed the calculus here is that request
*volume*, not per-write atomicity, turned out to be the thing actually
breaking runs. Merging in the read-only candidate list on top of that adds
some bytes to each batch write (`pbf_url` per entry), but that's tens of KB
even for a "hundreds of leaves" run, negligible next to the request-volume
problem that motivated batching in the first place, and irrelevant to it
(payload size and request count are independent costs).

The queue is scoped by `output_basename`, deliberately **not** by a run ID
on top of that: the finalize job resets the scope's file at the end of each
run, success or failure (`if: always()`), so retries always start from a
clean slate rather than resuming partway (kept simple on purpose: this
pipeline is a periodic full rebuild, not an incremental one, so there's no
resumability worth the extra bookkeeping). Because runs aren't run-ID-
scoped, two overlapping runs of the *same* build would otherwise race over
the same scope's file, so `_pipeline.yml` closes this itself with its own
top-level `concurrency: group:
tiledistillery-${{ github.repository }}-${{ inputs.output_basename }},
cancel-in-progress: false` (reusable `workflow_call` workflows can declare
`concurrency:` the same as any other workflow), so a caller doesn't have to
remember to serialize its own calls: it's enforced for every caller by
construction. This is also why `write_queue` can overwrite the scope's file
unconditionally (via `update_json_file_with_retry`, but only for its
"fetch current sha, then write" shape, not for genuine conflict retry):
nothing else can be writing this scope concurrently, and the prior run's
`cleanup` already reset whatever was there.

## Timing history & queue ordering

`state/timings/<output_basename>.json` on the caller's state branch (see
"State branch" below) holds `{ "<region-id>": [last up to 5 durations in
seconds] }`, and is written by exactly one job per run: `record-timings` in
`_pipeline.yml`. No worker writes it. Instead, `run-worker` (via
`claim.py`'s `_record_done`/`_record_failed`) appends each region's outcome
to a local per-worker file
(`--timings-buffer`); the `build` job uploads that file as an artifact the
same way it uploads shards; and `record-timings`, which runs once after
`build` finishes, downloads every worker's buffer and merges all of it into
the shared timing-history file **and** the shared queue file (see
"Locking": this is also what moves each region's id from `lock` into
`done`/`failed`) in one read-modify-write each
(`claim.py flush-timings --timings-dir`). It runs unconditionally
(`if: always()`), so a run that ultimately fails `verify-complete` still
keeps whatever timing data it produced, but `verify-complete` itself now
*waits* on this job (not just on `build`), since it's the one that applies
the claims half of that flush; timing history alone would have stayed
independent (it only feeds *future* runs' queue ordering, never this run's
correctness), but queue completeness is this run's correctness.

This exists because the file is shared and *not* keyed by its own ref the
way claims used to be (see "Locking"). A naive "every worker writes it
after every region" design turns it into a hot lock: with `worker_count`
workers each claiming tens of regions, that's hundreds of workers racing
the same read-modify-write over a run, and under a full fan-out that can
exhaust `update_json_file_with_retry`'s attempts outright, which used to
be a fatal error able to kill an otherwise-healthy worker's claim loop
mid-run (see `_record_done`'s docstring in `scripts/claim.py`). Funneling every
write through one job, once, doesn't just reduce that collision rate: it
removes the concurrent-writer problem this file has entirely, since after
`build` finishes there is nothing left running that could race
`record-timings`'s own single write. The queue file's own batched writes
during `build` (see "Locking") don't get this same "wait until nothing's
running" treatment: those still need to happen *during* the run, since
that's what claiming actually is. Only their `done`/`failed` half is
deferred this way.
`<output_basename>` here is the input's raw value, comma(s) and all when
building more than one layer together (e.g. `state/timings/bins,roads.json`):
building `bins,roads` together and calling this pipeline for `bins` alone
are different scopes with different histories, and that's intentional (see
below), not an oversight. Scoped this way for the same reason as the queue
(see "Locking") and for an additional reason of its own: build time is a
property of the *whole set* of configs sharing one download+build pass.
Building `bins` alone takes less time per region than building
`bins,roads` together, so their histories shouldn't mix, and building
`bins,roads` together isn't well-modeled as "the sum of bins's own history
and roads's own history" either, since the download is shared. Unlike the
queue, this file is *not* reset between runs, since history is exactly what's
meant to persist. The `prepare` job sorts the full leaf
list by mean recorded duration, **longest first**, before seeding
`state/queue/<scope>.json`'s `remaining` list with it (see "Locking"), so
the last regions claimed near the end of a run are the small ones. The
failure mode being avoided is one 30-minute outlier (e.g. Russia) still
running while every other worker has gone idle, which longest-first
ordering directly prevents (see "Region detection": oversized leaves are
deliberately *not* subdivided further, specifically so this
ordering, not finer sharding, is what absorbs the size skew).

A region with no timing history yet (new leaf, or the very first run) falls
back to sorting by its `.osm.pbf` byte size. Geofabrik's `index-v1.json`
gives URLs but no size field, so the `prepare` job does one `HEAD` request
per leaf (cheap: no body transferred, done once per run, not per worker) to
read `Content-Length`. Byte size is a proxy for duration, not a duration
itself; once a region has run once, its real timing data takes over on every
subsequent run.

## Worker job shape & the 6-hour limit

Each worker job stays well under GitHub's 6-hour job limit as long as
leaf-level (not country-or-larger) regions dominate the queue, which is the
point of taking Geofabrik's leaves rather than some coarser level. The
realistic risk is a handful of workers hitting the time wall (or crashing
outright) near the very end of a run while a few regions remain unclaimed
or stuck under a now-dead worker's `lock` (see "Locking"), not every worker
processing a full 6 hours' worth of tiny regions, since the queue simply
empties once every leaf is claimed.

There is deliberately no second, top-up claim-loop round to mop this up,
kept to one round on purpose (see "Distribution"): `fail-fast: false` means
one worker's bad ending never cancels the rest of the matrix, so the fleet
as a whole keeps draining the queue regardless of any single worker's fate,
and a stuck `lock` from the one worker that actually crashed mid-build
costs only the region(s) it was holding, not the run (see "Locking" for
that trade-off in full, and "Deliberately deferred" for the actual fix if
it's ever needed). The
`verify-complete` job (`if: always()`, so it still runs and reports
correctly even if one `build` leg failed at the job level) fails the run
loudly if any region is left neither `done` nor `failed`, rather than
letting `merge` silently publish a world layer with holes in it.

## Multiple layers, one download

`config`/`process`/`output_basename` each take one value, or a
comma-separated list matched 1:1 (e.g. `output_basename: bins,roads`), the
same convention TileAlchemist uses for its `profile`/`output_basename`
inputs, and for the same reason: a region's `.osm.pbf` download is the same
bytes regardless of which layer is being computed from them, so fetching it
once and running tilemaker once per config against those bytes, instead of
once per config *per call*, avoids repeating the one genuinely expensive,
rate-limit-sensitive part of the work. TileAlchemist's version of this
(see its `docs/ARCHITECTURE.md` "Parallelism") shares one HTTP range fetch
across profiles for the same reason; the mechanics differ (a Geofabrik
`.osm.pbf` download instead of a PMTiles range GET) but the shape of the
optimization is identical.

Concretely, one claimed region is one *shared* unit of work across every
`output_basename` in the call: `claim-and-build` (the composite action the
`build` job uses) downloads the region's `.osm.pbf` once, then loops over
the config/process/output_basename triples running tilemaker
against that same file, writing each layer's own
`<region-id>--<output_basename>.mbtiles`. If any one of those tilemaker
runs fails, the *whole* region is marked failed and retried as a whole
(all configs again, not just the one that failed): regions, not
individual configs, are the atomic unit of claiming and retrying, which is
simpler than tracking per-config partial success within one region and
matches the fact that they already share one claim. Any shard already
written by an earlier-succeeding config in that same loop is deleted
before the region is marked failed, so a permanently-failed region can
never contribute a partial shard to `merge`.

Since the claim is shared but each `output_basename` still needs its own
final `.pmtiles`, `merge` runs as its own matrix job, one cell per
`output_basename`, each globbing only its own `*--<output_basename>.mbtiles`
shards out of every worker's uploaded artifacts.

## Tile-build engine: tilemaker

**Decision: tilemaker**, not planetiler. Both are serious, actively
maintained, database-free OSM→vector-tile tools; this isn't a "one is
broken" call, it's a fit call for *this* architecture specifically:

- **The shard granularity already solves what planetiler is for.**
  Planetiler's headline advantage is whole-planet builds on a single
  machine (disk-spilling sort, ~1-1.5h/~100GB RAM for a full planet, vs.
  reported multi-hour/144GB-class runs for naive whole-planet tilemaker).
  That advantage doesn't apply here: this pipeline never builds the whole
  planet in one job, since that's the entire reason region detection exists.
  What each job actually needs to handle well is one *extract*, from a city
  up to an oversized country, and tilemaker's own documentation frames it
  as built for exactly that (explicitly *not* recommending itself for
  whole-planet builds without extra steps), while its RAM use for a single
  extract is smaller and more predictable at that scale. Use `--store
  <path>` (disk-backed node/way store) unconditionally in every worker, not
  just for known-large leaves, so a shard's peak memory doesn't depend on
  guessing which leaves are "big": cheap insurance on GitHub's public-repo
  runners (4 CPU / 16 GB RAM as of 2026). Revisit only if a specific
  oversized leaf (Russia-class) still can't fit.
- **Simpler, and *stable*, consumer-facing config.** A caller references a
  plain JSON layer config + a Lua process script by path: two files
  checked into the caller's own repo, no build step, directly analogous to
  TileAlchemist's "point at a `.py` profile" contract. Planetiler's
  closest equivalent, its YAML custom-schema format, is explicitly
  documented as unstable ("may change between releases," only a subset of
  the Java API exposed); planetiler's *stable* path is a compiled Java
  profile, which would mean either shipping a JVM build step in this
  pipeline or asking every caller repo to maintain and publish a JAR. Given
  the explicit goal that referencing this pipeline must be simple for a
  consumer, tilemaker's two-flat-files contract is the better fit today.
- **Output compatibility is a non-issue either way**: both tilemaker (since
  3.0) and planetiler write `.mbtiles` directly, and `tile-join`
  (tippecanoe) merges `.mbtiles` shards into one `.pmtiles`, matching
  TileAlchemist's existing merge job almost exactly. This criterion didn't
  end up deciding anything.
- **Ecosystem fit**: Geofabrik (the actual source of every extract this
  pipeline downloads) maintains its own tilemaker fork/deployment, some
  evidence the tool is well-exercised against exactly Geofabrik's extract
  shapes.

Revisit this if a specific consumer's config genuinely needs something
tilemaker's Lua tag processing can't express reasonably. That's a
per-config problem to solve if and when it appears, not a reason to
complicate every shard's engine now.

**Image: `ghcr.io/systemed/tilemaker:master`**, not Ubuntu's `apt` package.
It's upstream's own published image, so it carries current tilemaker
rather than whatever's frozen in Noble's universe repo (2.4.0, pre-3.0's
Lua API, missing since-fixed crashes). `:master` is the only tag
upstream's publish workflow produces (no semver tags), so this floats with
upstream's default branch instead of pinning an immutable release;
accepted since master is actively maintained, unlike the unmaintained apt
snapshot it replaces.

## Merge

Three jobs, after the claim-loop completes:

1. **`verify-complete`**, once per run regardless of how many
   `output_basename`s are being built (completeness is a property of
   *regions*, which are claimed once across the whole call, see "Multiple
   layers, one download"): confirms every expected leaf has a `done` claim
   (or a `failed` one, logged and, depending on how many, either
   tolerated or treated as a hard failure; exact threshold TBD when this is
   implemented, not an architectural question).
2. **`merge`**, matrixed one cell per `output_basename`: downloads every
   `shards-*` artifact, then runs one flat `tile-join
   --no-tile-size-limit --attribution=... -o <output_basename>.pmtiles
   shards/*--<output_basename>.mbtiles` for its own cell's basename,
   deliberately a single merge step, not staged-by-continent-then-globally,
   exactly like TileAlchemist's own merge job. A hierarchical merge would
   mean writing most tiles twice (once per continent, again in the final
   pass) and adds real complexity for a problem that doesn't exist yet;
   only revisit if the flat merge's memory/time in that one job actually
   becomes a bottleneck. Uploads `<output_basename>-pmtiles` as a workflow
   artifact. Pipeline still does not publish anywhere itself, same
   separation of concerns as TileAlchemist's `_pipeline.yml`.
3. **`cleanup`**, `if: always()`: resets this run's
   `state/queue/<output_basename>.json` to empty (logging any `failed`
   ones first), so the next run of the same build starts with a clean
   queue file regardless of how this one ended.

## State branch

One orphan branch, `state`, **in the calling repository** (see "State lives
in the caller's repo, not this one" above), not `main`, and not a branch
in `foxandfeature/tiledistillery`. Holds only:

| Path | What | Written by |
|---|---|---|
| `state/timings/<output_basename>.json` | last ≤5 durations per region-id, persists across runs | `record-timings` job, once per run |
| `state/queue/<output_basename>.json` | `{"remaining": [{"id", "pbf_url"}, ...], "lock": [...], "done": [...], "failed": [...]}` (see "Locking") | seeded by `prepare`; `remaining`/`lock` batched during `build`; `done`/`failed` by `record-timings`, once per run |

The queue file is reset to empty at the end of each run (see "Merge" step
3), then reseeded by the next run's `prepare` job; the timings file is not
reset, since history is what's meant to persist.

Shard binaries (`.mbtiles`) and the final `.pmtiles` are **never** committed
here, since they're GitHub Actions artifacts, exactly like TileAlchemist's shard
handling. The state branch only ever holds small JSON/refs, so it never
grows in a way that makes the repo unwieldy to clone.

## Publishing

Two reusable workflows, mirroring TileAlchemist's split and its reasoning:

- **`_publish-release.yml`**: downloads the `<output_basename>-pmtiles`
  artifact, splits into numbered parts if needed (GitHub's per-asset size
  limit), publishes/replaces a fixed-tag Release. Safe to call cross-repo:
  only uses the automatic `secrets.GITHUB_TOKEN` and
  `github.repository`/`github.run_number`, which reflect the *caller* even
  inside a called reusable workflow.
- **`_publish-b2.yml`**: mirrors the artifact to Backblaze B2, gated behind
  a `b2-publish` GitHub Environment (required reviewer) **and** a
  `github.repository == 'foxandfeature/tiledistillery'` job-level `if:`
  guard, not callable cross-repo, and not just by convention: a reusable
  workflow's `environment:` secrets resolve against the repo that *owns the
  workflow file*, not the caller, so without that guard any repository could
  reach this repo's real B2 credentials just by calling the file directly.
  A third-party adopter writes its own publish job with its own
  `environment:`/secrets instead.

Unlike TileAlchemist (which always publishes to both targets for its two
lightweight layers), the calling workflow here picks **one**, by comparing
the merged artifact's size against a threshold input: GitHub Releases when
it reasonably fits (even split into parts), Backblaze B2 otherwise, since a
whole-world layer merged from every Geofabrik leaf can plausibly outgrow
what's practical to split across Release assets, unlike TileAlchemist's
per-profile layers. The threshold is a caller-configurable input, not a
hardcoded constant, since this repo has no domain-specific layer to size that
number against.

## Consumer contract

`_pipeline.yml` (`on: workflow_call`) inputs:

- `config`, `process`: path(s) in the *caller's* repo to the tilemaker
  JSON config(s) and Lua process script(s): one value each, or a
  comma-separated list matched 1:1 with `output_basename` (see "Multiple
  layers, one download").
- `output_basename`: required, one value or a comma-separated list
  matched 1:1 with `config`/`process`.
- `attribution`: required, one value applied to every layer in the call.
- `worker_count`: default ~20 (see "Distribution").
- `region_scope`: optional Geofabrik path prefix filter (e.g.
  `europe/monaco`); default empty = every leaf, whole world. Exists so a
  caller (including this repo's own CI, see below) can run the pipeline
  against a tiny slice instead of triggering a full-world build every time.

Any repository calls it via
`uses: foxandfeature/tiledistillery/.github/workflows/_pipeline.yml@<ref>`,
a native cross-repo capability of reusable workflows, usable by any repo
from day one, not just internally, the same public contract TileAlchemist
already exposes for its own `_pipeline.yml`. One thing the caller's own
workflow must still provide, since `_pipeline.yml` deliberately never
touches any repository but the caller's own (see "State lives in the
caller's repo, not this one"): `permissions: contents: write` (job- or
workflow-level), needed to create/read/write the caller's own `state`
branch and its queue file. (Serializing repeated calls is *not* the
caller's job: `_pipeline.yml` enforces that itself, see "Locking".)

This repo's own `ci.yml` (see "CI self-test") shows the minimal caller shape.

## Trigger

`workflow_dispatch` (manual) plus a `schedule` cron, both on the calling
workflow, not `_pipeline.yml` itself (a reusable workflow can't declare its
own schedule).

## CI self-test

This repo's own caller workflow (`.github/workflows/ci.yml`) exercises the
full pipeline on two minimal dummy tilemaker configs *built together in one
call*: no domain-specific layers, just enough to prove config-referencing,
sharding, claiming, and merge work end to end, and specifically that the
comma-separated `config`/`process`/`output_basename` path (see "Multiple
layers, one download") produces two independent `<output_basename>.pmtiles`
outputs from one shared download per region. Scoped via `region_scope` to
something tiny (e.g. `europe/monaco`) so PR CI doesn't attempt a
full-world build. The first real consumer, `trashtracker-tiles`, is a
separate, later, out-of-scope repo.

## Deliberately deferred

- Exact `failed`-list tolerance threshold in `finalize` (how many
  permanently-stuck leaves still allow a "successful" run): an operational
  tuning question, not an architectural one; start strict (any failed leaf
  blocks the run) and loosen only if real Geofabrik data makes that
  impractical.
- Stale-claim reclaim for a worker that crashes mid-build without releasing
  its claimed batch's `lock` entries, deliberately not built (see
  "Locking"). A bigger `worker_count` does *not* fix this (it only helps a
  region that was never claimed in time, not one already claimed and then
  abandoned), so this is accepted as-is: a stuck batch costs the region(s)
  that one worker was holding (bounded by `--batch-size`), and the run
  needs a manual rerun (e.g. via `region_scope`) to pick those back up. The
  straightforward fix, if this turns out to be a real, recurring problem
  rather than a theoretical one: a lease/timestamp per `lock` entry, so
  another still-running worker can reclaim one after it goes stale within
  the same run, rather than only at the next run's `cleanup_claims.py`
  reset.
- Release-vs-B2 size threshold default: caller-tunable input, no default
  chosen here since this repo builds no real layer to size it against.
