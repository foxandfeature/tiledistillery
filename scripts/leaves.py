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
# effective_parent below for the general case this doesn't catch) and
# which index-v1.json itself has no flag for. Two groups, both download-only
# convenience bundles layered on top of extracts that are also independent
# leaves in their own right:
#   - the US Census regions (us-midwest, ...): declared parent
#     "north-america" instead of "us", so nothing marks them as sitting on
#     top of the individual `us/<state>` extracts.
#   - Geofabrik's own "Special Sub Regions" (alps, dach, ...): each
#     continent's download page (e.g. download.geofabrik.de/europe.html)
#     lists these separately under a "Special Sub Regions" heading —
#     "outside of the usual administrative hierarchies and may duplicate
#     data already contained in the other sub regions", in Geofabrik's own
#     words — but that grouping only exists in the HTML, not index-v1.json,
#     so it's transcribed here by hand (checked against every continent
#     page's `#specialsubregions` table).
#   - "enfield" is a one-off third case, not either of the above: it's the
#     *only* London borough Geofabrik publishes as its own leaf — every
#     other borough (Camden, Westminster, Hackney, ...) is only available
#     bundled in "greater-london" itself. `parent: "greater-london"` is
#     correct, but it means the general "a referenced parent is coarser,
#     drop it" rule below would silently exclude "greater-london" for
#     having a child, without that one child covering anywhere near the
#     whole area — the run would fetch tiny Enfield and quietly never
#     fetch the rest of London at all. See find_leaves() for the other
#     half of this fix (a redundant leaf must not keep shadowing its own
#     parent).
KNOWN_REDUNDANT_LEAVES = {
    "us-midwest", "us-northeast", "us-pacific", "us-south", "us-west",
    "alps", "britain-and-ireland", "dach", "great-britain",
    "south-africa-and-lesotho", "sea", "kaliningrad",
    "enfield",
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
    # A redundant leaf (see KNOWN_REDUNDANT_LEAVES, e.g. "enfield") must not
    # count towards making *its own parent* look non-leaf: if it did,
    # excluding "enfield" would leave "greater-london" excluded too (still
    # "referenced as a parent"), so neither ever gets fetched.
    parents_referenced = {
        effective_parent(gid, by_id) for gid in by_id
        if gid not in KNOWN_REDUNDANT_LEAVES
    }
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
