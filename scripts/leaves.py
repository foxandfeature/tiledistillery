#!/usr/bin/env python3
"""Build the run's queue manifest: every Geofabrik leaf extract in scope,
ordered longest-first. See docs/ARCHITECTURE.md "Region detection" and
"Timing history & queue ordering".

A leaf's region-id is its full path through the Geofabrik parent chain
(e.g. "europe/germany/bavaria"), not Geofabrik's own short `id` field
(e.g. "bavaria") — ids are unique on their own, but the path is what lets
--region-scope filter a whole subtree by prefix, and reads far better as a
git ref / artifact name.
"""

import argparse
import concurrent.futures
import json
import statistics
import sys

import requests

import common

INDEX_URL = "https://download.geofabrik.de/index-v1.json"


def fetch_index():
    resp = requests.get(INDEX_URL, timeout=60)
    resp.raise_for_status()
    return resp.json()["features"]


# Geofabrik regional bundles that overlap finer extracts already covered
# elsewhere in the tree, but whose `parent` field doesn't say so (see
# effective_parent below for the general case this doesn't catch): these
# US Census-region groupings (id, declared parent "north-america") sit
# geographically on top of the individual `us/<state>` extracts, which
# are also independent leaves — download-only convenience bundles Geofabrik
# offers alongside the finer split, not a coarser tier of the same tree.
# Nothing in the index marks that redundancy, so it's hardcoded here.
KNOWN_REDUNDANT_LEAVES = {
    "us-midwest", "us-northeast", "us-pacific", "us-south", "us-west",
}


def effective_parent(gid, by_id):
    """Geofabrik's `parent` field is supposed to encode the containment
    tree find_leaves() relies on, but for every `us/<state>` extract it
    points straight at "north-america" instead of "us" — even though the
    id itself already encodes that nesting with a literal "/". Trusting
    `parent` there makes "us" (whole country) look like a leaf alongside
    every state that's actually inside it, double-covering the whole US.
    Where an id's own slash-prefix names another real feature, treat that
    as the true parent instead of the declared one; otherwise fall back to
    `parent` as normal (this is a data quirk isolated to the `us/*` branch
    today, not a general Geofabrik convention, so it only ever overrides
    anything for ids shaped like that).
    """
    if "/" in gid:
        prefix = gid.rsplit("/", 1)[0]
        if prefix in by_id:
            return prefix
    return by_id[gid].get("parent")


def compute_paths(features):
    """Returns {geofabrik_id: full/slash/path}."""
    by_id = {f["properties"]["id"]: f["properties"] for f in features}
    paths = {}

    def path_of(gid, _seen=None):
        if gid in paths:
            return paths[gid]
        _seen = _seen or set()
        if gid in _seen:
            raise ValueError(f"cycle in Geofabrik parent chain at {gid!r}")
        _seen.add(gid)
        parent = effective_parent(gid, by_id)
        # When the parent came from gid's own slash-prefix (see
        # effective_parent), gid already spells out that prefix itself
        # (e.g. "us/wisconsin"'s effective parent is "us") — append only
        # the part after it, or the parent's path would be duplicated
        # into the result ("north-america/us/us/wisconsin").
        local = gid[len(parent) + 1:] if parent and gid.startswith(parent + "/") else gid
        result = f"{path_of(parent, _seen)}/{local}" if parent else gid
        paths[gid] = result
        return result

    for gid in by_id:
        path_of(gid)
    return paths


def find_leaves(features):
    by_id = {f["properties"]["id"]: f["properties"] for f in features}
    parents_referenced = {effective_parent(gid, by_id) for gid in by_id}
    return [
        f for f in features
        if f["properties"]["id"] not in parents_referenced
        and f["properties"]["id"] not in KNOWN_REDUNDANT_LEAVES
    ]


def in_scope(path, region_scope):
    if not region_scope:
        return True
    return path == region_scope or path.startswith(region_scope + "/")


def pbf_size(url):
    """One HEAD request; used only for leaves with no timing history yet."""
    resp = requests.head(url, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    length = resp.headers.get("Content-Length")
    return int(length) if length is not None else 0


def fetch_pbf_sizes(urls, max_workers=16):
    """Parallel HEAD requests for leaves with no timing history yet. On a
    fresh run (empty timings.json) this is hundreds of leaves, and doing
    them one at a time was the dominant cost of building the manifest."""
    sizes = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_url = {pool.submit(pbf_size, url): url for url in set(urls)}
        for future in concurrent.futures.as_completed(future_to_url):
            sizes[future_to_url[future]] = future.result()
    return sizes


def timings_path(output_basename):
    return f"state/timings/{output_basename}.json"


def build_manifest(region_scope, repo, token, state_branch, output_basename):
    features = fetch_index()
    paths = compute_paths(features)
    leaves = find_leaves(features)

    timings, _ = common.get_json_file(repo, token, state_branch, timings_path(output_basename))

    candidates = []
    for feature in leaves:
        props = feature["properties"]
        path = paths[props["id"]]
        if not in_scope(path, region_scope):
            continue
        pbf_url = props.get("urls", {}).get("pbf")
        if not pbf_url:
            print(f"::warning::skipping {path!r}: no .osm.pbf URL in Geofabrik index", file=sys.stderr)
            continue
        candidates.append((path, pbf_url, timings.get(path) or []))

    sizes = fetch_pbf_sizes(pbf_url for _, pbf_url, durations in candidates if not durations)

    regions = []
    for path, pbf_url, durations in candidates:
        if durations:
            regions.append({
                "id": path,
                "pbf_url": pbf_url,
                "has_history": True,
                "sort_metric": statistics.mean(durations),
            })
        else:
            regions.append({
                "id": path,
                "pbf_url": pbf_url,
                "has_history": False,
                "sort_metric": sizes[pbf_url],
            })

    # Primary: known timing history sorts before size-only estimates (see
    # docs/ARCHITECTURE.md "Timing history" — the two metrics are different
    # units, sorting them as one numeric range would be meaningless).
    # Secondary, within each group: longest/largest first.
    regions.sort(key=lambda r: (not r["has_history"], -r["sort_metric"]))

    return {"region_scope": region_scope, "regions": regions}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region-scope", default="")
    parser.add_argument("--repo", required=True, help="owner/repo of the calling repository (state lives on its own 'state' branch)")
    parser.add_argument("--token", required=True)
    parser.add_argument("--state-branch", default="state")
    parser.add_argument("--output-basename", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    manifest = build_manifest(
        args.region_scope, args.repo, args.token, args.state_branch, args.output_basename,
    )
    with open(args.out, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"{len(manifest['regions'])} region(s) in scope {args.region_scope!r}, written to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
