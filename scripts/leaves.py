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
        parent = by_id[gid].get("parent")
        result = f"{path_of(parent, _seen)}/{gid}" if parent else gid
        paths[gid] = result
        return result

    for gid in by_id:
        path_of(gid)
    return paths


def find_leaves(features):
    parents_referenced = {f["properties"].get("parent") for f in features}
    return [f for f in features if f["properties"]["id"] not in parents_referenced]


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


def timings_path(output_basename):
    return f"state/timings/{output_basename}.json"


def build_manifest(region_scope, repo, token, state_branch, output_basename):
    features = fetch_index()
    paths = compute_paths(features)
    leaves = find_leaves(features)

    timings, _ = common.get_json_file(repo, token, state_branch, timings_path(output_basename))

    regions = []
    for feature in leaves:
        props = feature["properties"]
        path = paths[props["id"]]
        if not in_scope(path, region_scope):
            continue
        pbf_url = props.get("urls", {}).get("pbf")
        if not pbf_url:
            print(f"::warning::skipping {path!r}: no .osm.pbf URL in Geofabrik index", file=sys.stderr)
            continue

        durations = timings.get(path) or []
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
                "sort_metric": pbf_size(pbf_url),
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
