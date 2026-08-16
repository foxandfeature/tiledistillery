# Architecture

TileDistillery is a reusable GitHub Actions pipeline that turns raw
[Geofabrik](https://download.geofabrik.de) OSM extracts into finished
PMTiles vector-tile layers, using [tilemaker](https://github.com/systemed/tilemaker)
(see "Tile-build engine" below) against a config a *caller* repo supplies.
Unlike [TileAlchemist](https://github.com/foxandfeature/tilealchemist) — a
sibling project solving an adjacent problem, not a dependency of this one —
there is no profile system here: a caller points the pipeline at its own
tilemaker JSON config + Lua process script, the same way TileAlchemist
callers point at a Python profile file. TileDistillery itself never ships a
domain-specific config; only a minimal dummy one for CI self-tests (see
"CI self-test").

TileAlchemist's `docs/ARCHITECTURE.md` describes a similar-shaped problem —
spreading work across many GitHub Actions runners, merging shards, publishing
to two targets — and several of its patterns reappear below (reusable
pipeline workflow separated from publishing; small isolated worker jobs to
respect the 6-hour job limit; a cross-repo-safe Releases path plus a
repo-guarded B2 path). Its actual sharding mechanism does not: TileAlchemist
splits a single PMTiles archive into equal contiguous byte ranges by offset,
which only works because a PMTiles directory gives you exact tile
offsets/lengths up front. There is no equivalent here — a Geofabrik OSM
extract has no analogous fixed-size index — so distribution is a claim-based
queue over heterogeneously-sized regions instead (see below), not a
byte-range partition.

## Region detection

`scripts/leaves.py` fetches Geofabrik's `index-v1.json`, builds the
parent/child tree, and takes every node with no children as one shard
("leaf"): the smallest extract Geofabrik publishes along that branch (e.g.
`europe/germany/bavaria`, not all of `europe/germany`). This is the
granularity of a work item everywhere below — "region" always means "one
leaf." Leaves are not merged or split any further; an oversized leaf (Russia)
and a tiny one (Vatican) are both single, indivisible units of work (see
"Timing history" for how that asymmetry is handled).

## Distribution: dynamic claim queue, not a static matrix

A static `strategy: matrix` over ~hundreds of leaves would technically run
to completion, but gives up exactly the property this pipeline needs: which
matrix cell GitHub Actions starts next, as slots free up, isn't a documented
contract, so there's no reliable way to make it start the longest-running
regions first. It would also pay full job startup cost (checkout, tilemaker
binary, Geofabrik download setup) once per *leaf*, which is wasteful when
most leaves are small and there are hundreds of them.

Instead, `_pipeline.yml` starts a fixed number of long-lived **worker**
jobs (`worker_count` input, default ~20 — GitHub already queues more than
~20 concurrently *running* jobs on a public repo, so a higher count doesn't
add real parallelism, only job count; see TileAlchemist's ARCHITECTURE.md
"Parallelism" for the same reasoning). Each worker runs a loop, not a single
region:

1. Fetch the current claim state (see "Locking" below) and the precomputed,
   duration-sorted candidate list (see "Timing history").
2. Attempt to claim the best remaining candidate — one claim per region,
   never per `output_basename` (see "Locking" for why that matters when a
   call builds more than one layer together).
3. On success: download that leaf's `.osm.pbf` **once**, run tilemaker
   once per `output_basename` in this call against those same bytes (see
   "Multiple layers, one download" below), upload each resulting
   `<region-id>--<output_basename>.mbtiles`, record the claim as done and
   append this run's duration to the timing history.
4. Loop back to 1. Exit when no claimable candidate remains, or the job's
   own time budget runs low.

This is the literal implementation of "a worker requests the next region as
soon as it's done": the next claim happens inside the same job, immediately,
with the tilemaker binary and checkout already warm, not via a fresh job
dispatch per region.

## State lives in the caller's repo, not this one

The state branch (claims + timing history, see below) lives on a `state`
branch **in the calling repository**, not centrally in
`foxandfeature/tiledistillery`. This isn't just tidiness: the automatic
`secrets.GITHUB_TOKEN` a workflow run gets is scoped to the repository the
workflow is running in, with no cross-repo write access — a third-party
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
`output_basename` within that repo's own branch — see below.

## Locking: git refs as the claim mechanism

Up to ~20 workers can race to claim the same region. The mechanism is atomic
git ref creation, one ref per region, under a scope prefix identifying
*which build* (within this repo) is running:
`refs/claims/<output_basename>/<region-id>/...` — e.g. a `bins` layer build
claims `refs/claims/bins/europe/germany/bavaria/...`, and a call building
`bins` and `roads` together (see "Multiple layers, one download" below)
claims `refs/claims/bins,roads/europe/germany/bavaria/...` — the raw input
value, comma(s) and all, is the scope; it isn't split apart or deduplicated
against a separate `bins`-only build's own scope, since those really are
different builds with independent claim/timing history. Region IDs are
themselves slash-separated Geofabrik paths (e.g. `europe/germany/bavaria`),
so the whole ref is just nested path segments — a valid git ref name, no
encoding needed beyond stripping characters git refs disallow.

Creating a ref via the Git Data API (`POST /repos/.../git/refs`) is atomic
server-side and fails with 422 if the ref already exists — a real
compare-and-swap, not a convention two workers could both "follow" into a
race, unlike a shared-file queue that every worker reads-modifies-writes
(that needs its own retry-on-conflict loop for every claim, not just
contended ones). But that atomicity only holds if the ref *name* being
created is identical for every worker racing over the same region — so the
lock ref itself must have a **fixed name per region**, with no timestamp or
other varying component in it: `.../<region-id>/lock`. (An earlier version
of this put the claim timestamp directly in the lock ref's name, e.g.
`.../lock/<unix-timestamp>`, which silently defeats the whole mechanism:
two workers claiming the same region a second apart would each create a
*different*, non-colliding ref name, and both would believe they'd won.)

Rather than encoding claim metadata (status, timestamp, attempt count) in
the pointed-to commit's content — which would cost extra API round-trips to
read back for every sweep — that metadata lives directly in ref names, so
listing refs under the scope prefix once
(`GET /git/matching-refs/claims/<scope>`) is enough to know the full state:

- **Claiming**: create `.../<region-id>/lock` (atomic; loses the race →
  422 → try the next candidate). Having won, the same worker then creates
  a *second*, companion ref, `.../<region-id>/lock-at/<unix-timestamp>` —
  purely so a later sweep can tell how old the claim is from the ref name
  alone. This second create is never itself contested: only the worker
  that just won `.../lock` ever reaches it. Every ref points at the state
  branch's current tip commit — the commit content is irrelevant, only
  each ref's existence and name matter, so no extra blob/tree/commit needs
  constructing per claim.
- **Done**: create `.../<region-id>/done` and delete both `.../lock` and
  `.../lock-at/*` (no race on any of this — only *creating* a
  not-yet-existing ref is contested; deleting refs a worker's own claim
  already owns isn't).
- **Failed**: create `.../<region-id>/attempt/<n>` and delete
  `.../lock` + `.../lock-at/*`; past 3 attempts, also create
  `.../<region-id>/failed` and stop reclaiming it — a permanently-stuck
  region needs a human, not more retries.
- **Stale reclaim**: before claiming a *new* region, a worker sweeps
  existing `.../lock` refs in scope for a `lock-at` companion older than a
  TTL (a generous flat ceiling, e.g. 90 minutes, until real per-region
  timing data suggests a tighter one) — or *missing* entirely, which is
  treated as stale immediately rather than un-reclaimable, since a crash
  landing between the two creates is exactly the kind of thing this sweep
  exists to recover from. Releases it and records an `attempt/<n>`, the
  same as an explicit failure. This makes crash recovery (a worker's job
  cancelled or OOM-killed mid-build, never reaching `done`) self-healing
  through the normal claim loop, with no separate sweep job.

Why refs and not, say, a shared `queue.json` committed to the state branch:
every worker touching the same file still races at the branch-tip level
(two concurrent commits, one gets rejected), so it needs the same
retry-on-conflict handling as ref creation *without* the atomic
create-if-absent guarantee refs give for free. Why not the Actions cache or
an Issues/labels-based lock: neither gives atomic create-if-not-exists
semantics as directly, and both add an extra system when git already has
one that fits. `state/timings/...json` (below) *is* a shared committed
file, because it's low-frequency (once per region per run, not once per
claim attempt) and idempotent to retry — the ref mechanism is specifically
for the part that's genuinely racy.

Claim refs are scoped by `output_basename`, deliberately **not** by a run ID
on top of that: the finalize job deletes every ref under the scope prefix at
the end of each run — success or failure (`if: always()`) — so retries
always start from a clean slate rather than resuming partway (kept simple
on purpose: this pipeline is a periodic full rebuild, not an incremental
one, so there's no resumability worth the extra bookkeeping). Because runs
aren't run-ID-scoped, two overlapping runs of the *same* build would
otherwise race over the same scope prefix — `_pipeline.yml` closes this
itself with its own top-level `concurrency: group:
tiledistillery-${{ github.repository }}-${{ inputs.output_basename }},
cancel-in-progress: false` (reusable `workflow_call` workflows can declare
`concurrency:` the same as any other workflow), so a caller doesn't have to
remember to serialize its own calls — it's enforced for every caller by
construction.

## Timing history & queue ordering

`state/timings/<output_basename>.json` on the caller's state branch (see
"State branch" below) holds `{ "<region-id>": [last up to 5 durations in
seconds] }`. Each worker buffers its own regions' durations locally
(`claim.py done --timings-buffer`) and merges its whole buffer into this
shared file in one write at the end of its run (`claim.py flush-timings`,
triggered by a trap in `claim-and-build/action.yml` so it fires however the
worker's loop ends), rather than writing this file on every single `done` —
see `cmd_done`'s docstring in `scripts/claim.py` for why: with
`worker_count` workers each claiming tens of regions, a write per region
turns this single shared file into a hot lock that plausibly exhausts
`update_json_file_with_retry`'s attempts, which used to be a fatal error
able to kill an otherwise-healthy worker's claim loop mid-run. Batching to
one write per worker cuts the collision rate on this file by roughly the
average regions-per-worker count, which is what keeps this
shared-file-with-retry approach viable at this concurrency, not just a
smaller blast radius when it fails.
`<output_basename>` here is the input's raw value, comma(s) and all when
building more than one layer together (e.g. `state/timings/bins,roads.json`)
— building `bins,roads` together and calling this pipeline for `bins` alone
are different scopes with different histories, and that's intentional (see
below), not an oversight. Scoped this way for the same reason as claims
(see "Locking") and for an additional reason of its own: build time is a
property of the *whole set* of configs sharing one download+build pass —
building `bins` alone takes less time per region than building
`bins,roads` together, so their histories shouldn't mix, and building
`bins,roads` together isn't well-modeled as "the sum of bins's own history
and roads's own history" either, since the download is shared. Unlike
claims, this file is *not* reset between runs — history is exactly what's
meant to persist. The `prepare` job sorts the full leaf
list by mean recorded duration, **longest first**, so the last regions
claimed near the end of a run are the small ones — the failure mode being
avoided is one 30-minute outlier (e.g. Russia) still running while every
other worker has gone idle, which longest-first ordering directly prevents
(oversized leaves are deliberately *not* subdivided further — see "Region
detection" — specifically so this ordering, not finer sharding, is what
absorbs the size skew).

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
realistic risk is a handful of workers hitting the time wall near the very
end of a run while a few regions remain unclaimed — not every worker
processing a full 6 hours' worth of tiny regions, since the queue simply
empties once every leaf is claimed. `check-complete` (see "Merge") checks
for exactly this gap and, if regions remain, `topup` dispatches one bounded
second worker round before `merge`, capped at 2 rounds total; the
`verify-complete` job fails the run loudly if coverage is still incomplete
after that, rather than letting `merge` silently publish a world layer with
holes in it.

## Multiple layers, one download

`config`/`process`/`output_basename` each take one value, or a
comma-separated list matched 1:1 (e.g. `output_basename: bins,roads`) — the
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
`output_basename` in the call: `claim-and-build` (the composite action
`build`/`topup` both use) downloads the region's `.osm.pbf` once, then
loops over the config/process/output_basename triples running tilemaker
against that same file, writing each layer's own
`<region-id>--<output_basename>.mbtiles`. If any one of those tilemaker
runs fails, the *whole* region is marked failed and retried as a whole
(all configs again, not just the one that failed) — regions, not
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
  machine (disk-spilling sort, ~1–1.5h/~100GB RAM for a full planet, vs.
  reported multi-hour/144GB-class runs for naive whole-planet tilemaker).
  That advantage doesn't apply here: this pipeline never builds the whole
  planet in one job — that's the entire reason region detection exists.
  What each job actually needs to handle well is one *extract*, from a city
  up to an oversized country, and tilemaker's own documentation frames it
  as built for exactly that (explicitly *not* recommending itself for
  whole-planet builds without extra steps), while its RAM use for a single
  extract is smaller and more predictable at that scale. Use `--store
  <path>` (disk-backed node/way store) unconditionally in every worker, not
  just for known-large leaves, so a shard's peak memory doesn't depend on
  guessing which leaves are "big" — cheap insurance on GitHub's public-repo
  runners (4 CPU / 16 GB RAM as of 2026), revisit only if a specific
  oversized leaf (Russia-class) still can't fit.
- **Simpler, and *stable*, consumer-facing config.** A caller references a
  plain JSON layer config + a Lua process script by path — two files
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
  TileAlchemist's existing merge job almost exactly — this criterion didn't
  end up deciding anything.
- **Ecosystem fit**: Geofabrik — the actual source of every extract this
  pipeline downloads — maintains its own tilemaker fork/deployment, some
  evidence the tool is well-exercised against exactly Geofabrik's extract
  shapes.

Revisit this if a specific consumer's config genuinely needs something
tilemaker's Lua tag processing can't express reasonably — that's a
per-config problem to solve if and when it appears, not a reason to
complicate every shard's engine now.

## Merge

Three jobs, after the (possibly topped-up) claim-loop completes:

1. **`verify-complete`**, once per run regardless of how many
   `output_basename`s are being built (completeness is a property of
   *regions*, which are claimed once across the whole call — see "Multiple
   layers, one download"): confirms every expected leaf has a `done` claim
   (or a `failed` one, logged and — depending on how many — either
   tolerated or treated as a hard failure; exact threshold TBD when this is
   implemented, not an architectural question).
2. **`merge`**, matrixed one cell per `output_basename`: downloads every
   `shards-*` artifact, then runs one flat `tile-join
   --no-tile-size-limit --attribution=... -o <output_basename>.pmtiles
   shards/*--<output_basename>.mbtiles` for its own cell's basename —
   deliberately a single merge step, not staged-by-continent-then-globally,
   exactly like TileAlchemist's own merge job. A hierarchical merge would
   mean writing most tiles twice (once per continent, again in the final
   pass) and adds real complexity for a problem that doesn't exist yet;
   only revisit if the flat merge's memory/time in that one job actually
   becomes a bottleneck. Uploads `<output_basename>-pmtiles` as a workflow
   artifact — pipeline still does not publish anywhere itself, same
   separation of concerns as TileAlchemist's `_pipeline.yml`.
3. **`cleanup`**, `if: always()`: deletes every ref under this run's
   `refs/claims/<output_basename>/` scope (logging any `failed` ones
   first), so the next run of the same build starts with an empty claim
   namespace regardless of how this one ended.

## State branch

One orphan branch, `state`, **in the calling repository** (see "State lives
in the caller's repo, not this one" above) — not `main`, and not a branch
in `foxandfeature/tiledistillery`. Holds only:

| Path | What | Written by |
|---|---|---|
| `state/timings/<output_basename>.json` | last ≤5 durations per region-id, persists across runs | each worker, on `done` |
| `refs/claims/<output_basename>/<region-id>/lock` | in-progress claim, the actual mutex (fixed name — see "Locking") | claimed on create, deleted on done/failed/stale-reclaim |
| `refs/claims/<output_basename>/<region-id>/lock-at/<ts>` | companion to `lock`, claim age for staleness sweeps | created right after `lock`, deleted alongside it |
| `refs/claims/<output_basename>/<region-id>/done` | completed this run | claim/done step |
| `refs/claims/<output_basename>/<region-id>/attempt/<n>` | one per failed/stale attempt | reclaim sweep or explicit failure |
| `refs/claims/<output_basename>/<region-id>/failed` | exhausted 3 attempts | reclaim sweep, surfaced to finalize |

All of the above under one run's scope prefix is deleted at the end of that
run (see "Merge" step 5) except the timings file, which is what's meant to
persist.

Shard binaries (`.mbtiles`) and the final `.pmtiles` are **never** committed
here — they're GitHub Actions artifacts, exactly like TileAlchemist's shard
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
  guard — not callable cross-repo, and not just by convention: a reusable
  workflow's `environment:` secrets resolve against the repo that *owns the
  workflow file*, not the caller, so without that guard any repository could
  reach this repo's real B2 credentials just by calling the file directly.
  A third-party adopter writes its own publish job with its own
  `environment:`/secrets instead.

Unlike TileAlchemist (which always publishes to both targets for its two
lightweight layers), the calling workflow here picks **one**, by comparing
the merged artifact's size against a threshold input: GitHub Releases when
it reasonably fits (even split into parts), Backblaze B2 otherwise — a
whole-world layer merged from every Geofabrik leaf can plausibly outgrow
what's practical to split across Release assets, unlike TileAlchemist's
per-profile layers. The threshold is a caller-configurable input, not a
hardcoded constant — this repo has no domain-specific layer to size that
number against.

## Consumer contract

`_pipeline.yml` (`on: workflow_call`) inputs:

- `config`, `process` — path(s) in the *caller's* repo to the tilemaker
  JSON config(s) and Lua process script(s): one value each, or a
  comma-separated list matched 1:1 with `output_basename` — see "Multiple
  layers, one download".
- `output_basename` — required, one value or a comma-separated list
  matched 1:1 with `config`/`process`.
- `attribution` — required, one value applied to every layer in the call.
- `worker_count` — default ~20 (see "Distribution").
- `region_scope` — optional Geofabrik path prefix filter (e.g.
  `europe/monaco`); default empty = every leaf, whole world. Exists so a
  caller (including this repo's own CI, see below) can run the pipeline
  against a tiny slice instead of triggering a full-world build every time.

Any repository calls it via
`uses: foxandfeature/tiledistillery/.github/workflows/_pipeline.yml@<ref>`,
a native cross-repo capability of reusable workflows — usable by any repo
from day one, not just internally, the same public contract TileAlchemist
already exposes for its own `_pipeline.yml`. One thing the caller's own
workflow must still provide, since `_pipeline.yml` deliberately never
touches any repository but the caller's own (see "State lives in the
caller's repo, not this one"): `permissions: contents: write` (job- or
workflow-level), needed to create/read/delete the caller's own `state`
branch and its claim refs. (Serializing repeated calls is *not* the
caller's job — see "Locking" — `_pipeline.yml` enforces that itself.)

This repo's own `ci.yml` (see "CI self-test") shows the minimal caller shape.

## Trigger

`workflow_dispatch` (manual) plus a `schedule` cron, both on the calling
workflow, not `_pipeline.yml` itself (a reusable workflow can't declare its
own schedule).

## CI self-test

This repo's own caller workflow (`.github/workflows/ci.yml`) exercises the
full pipeline on two minimal dummy tilemaker configs *built together in one
call* — no domain-specific layers, just enough to prove config-referencing,
sharding, claiming, and merge work end to end, and specifically that the
comma-separated `config`/`process`/`output_basename` path (see "Multiple
layers, one download") produces two independent `<output_basename>.pmtiles`
outputs from one shared download per region. Scoped via `region_scope` to
something tiny (e.g. `europe/monaco`) so PR CI doesn't attempt a
full-world build. The first real consumer, `trashtracker-tiles`, is a
separate, later, out-of-scope repo.

## Deliberately deferred

- Exact `refs/claims-failed` tolerance threshold in `finalize` (how many
  permanently-stuck leaves still allow a "successful" run) — an operational
  tuning question, not an architectural one; start strict (any failed leaf
  blocks the run) and loosen only if real Geofabrik data makes that
  impractical.
- Exact stale-claim TTL (currently a flat placeholder) — revisit once real
  per-region timing data exists to set a tighter, per-region-aware bound.
- Release-vs-B2 size threshold default — caller-tunable input, no default
  chosen here since this repo builds no real layer to size it against.
