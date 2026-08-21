# TileDistillery

A reusable GitHub Actions pipeline that builds PMTiles vector-tile layers
from raw [Geofabrik](https://download.geofabrik.de) OSM extracts, using
[tilemaker](https://github.com/systemed/tilemaker). A sibling project to
[TileAlchemist](https://github.com/foxandfeature/tilealchemist), a related
problem, but architecturally independent; neither is input or output for
the other.

Unlike TileAlchemist, there's no profile system here: a caller repo brings
its own native tilemaker JSON config + Lua process script and calls this
pipeline with a reference to them. TileDistillery itself ships no
domain-specific layer, only a minimal dummy config used by its own CI (see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) "CI self-test").

## Using it from your own repo

```yaml
name: Build my layer

on:
  workflow_dispatch:
  schedule:
    - cron: "0 3 * * 0"

jobs:
  build:
    uses: foxandfeature/tiledistillery/.github/workflows/_pipeline.yml@main
    permissions:
      contents: write   # this pipeline keeps its claim/timing state on YOUR repo's own `state` branch
    with:
      config: tiles/config.json
      process: tiles/process.lua
      output_basename: my-layer
      attribution: "My Layer, data (c) OpenStreetMap contributors, ODbL"
      # region_scope: "europe/germany"   # optional: restrict to a subtree instead of the whole world
      # worker_count: "20"               # optional: default 20
      #
      # Building more than one layer? Pass matching comma-separated lists
      # instead: one shared .osm.pbf download per region across all of
      # them (see docs/ARCHITECTURE.md "Multiple layers, one download"):
      #   config: tiles/bins/config.json,tiles/roads/config.json
      #   process: tiles/bins/process.lua,tiles/roads/process.lua
      #   output_basename: bins,roads

  publish:
    needs: build
    uses: foxandfeature/tiledistillery/.github/workflows/_publish-release.yml@main
    permissions:
      contents: write
    with:
      output_basename: my-layer
      tag: my-layer-latest
      title: "My Layer"
```

`_pipeline.yml` never publishes anywhere itself. Pick a publish target
(GitHub Releases, shown above, or your own Backblaze B2 job) in your own
calling workflow. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
"Publishing" and "Consumer contract" for the full picture, including why
`contents: write` is the only permission you need to grant.

The first real consumer, a future `trashtracker-tiles` repo (a trash-bin
layer from zoom 10), is separate and not part of this repository.

## Architecture

[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) covers region detection off
the Geofabrik index, the dynamic claim-based work queue (git refs as an
atomic distributed lock, no static job matrix), timing-history-based
longest-first ordering, the tilemaker-vs-planetiler decision, and
publishing.

## Repository layout

| Path | Purpose |
| --- | --- |
| `docs/ARCHITECTURE.md` | Region detection, distribution, locking, merge, tile-engine decision, publishing. |
| `.github/workflows/_pipeline.yml` | Reusable: builds one PMTiles layer from every Geofabrik leaf in scope. Never publishes. Safe to call cross-repo. |
| `.github/workflows/_publish-release.yml` | Reusable: publishes a merged `.pmtiles` artifact as a GitHub Release. Safe to call cross-repo. |
| `.github/workflows/_publish-b2.yml` | Reusable: mirrors a merged `.pmtiles` artifact to Backblaze B2. Repo-internal only. |
| `.github/workflows/ci.yml` | This repo's own self-test: calls `_pipeline.yml` against `dummy/`, scoped to one small leaf. |
| `.github/actions/claim-and-build/` | Composite action: installs tilemaker, then loops claiming and building regions until the queue is empty. Shared by `_pipeline.yml`'s main and top-up rounds. |
| `scripts/leaves.py` | Parses the Geofabrik index into leaf extracts, builds the run's ordered queue manifest. |
| `scripts/claim.py` | The claim lifecycle (`next`/`done`/`failed`) over the git-ref locking scheme. |
| `scripts/finalize_check.py` | Compares a run's manifest against claim-ref state; the pre-merge completeness gate. |
| `scripts/cleanup_claims.py` | Deletes a run's claim refs at the end of `_pipeline.yml`, always. |
| `scripts/common.py` | Shared GitHub API helpers (ref create/delete/list, Contents API with retry). |
| `dummy/` | Two minimal tilemaker configs used only by `ci.yml` (built together, to exercise the shared-download multi-layer path); not an example to copy for a real layer. |

## Contributing

Bug reports and pull requests are welcome.

## License / attribution

The code in this repository is licensed under the MIT License. See
[`LICENSE`](LICENSE). Map data built *with* this pipeline (Geofabrik/OSM
extracts and anything derived from them) is not: OpenStreetMap data is
© OpenStreetMap contributors, available under the
[Open Database License (ODbL)](https://www.openstreetmap.org/copyright).
