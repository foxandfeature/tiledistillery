#!/usr/bin/env python3
"""Seeds the run's queue: every Geofabrik leaf extract in scope, ordered
longest-first, written straight to state/queue/<scope>.json on the calling
repository's own `state` branch (see write_queue). See
docs/ARCHITECTURE.md "Region detection" and "Timing history & queue
ordering".

A leaf's region-id is its full path through Geofabrik's own declared
parent chain (see compute_paths), not Geofabrik's own short `id` field;
see docs/ARCHITECTURE.md "Region detection" for why (--region-scope
prefix filtering, state/timings/ keys).

--region-scope accepts a comma-separated list of prefixes (in_scope
matches if any one of them matches), not just a single prefix, so a
caller can restrict a run to several disjoint subtrees at once.
"""

import argparse
import concurrent.futures
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
# elsewhere in the tree, but which index-v1.json itself has no flag for,
# so this is transcribed by hand. Three distinct cases (sources, dates,
# and the "enfield" reasoning in full: see docs/ARCHITECTURE.md "Region
# detection"):
#   - Geofabrik's own "Special Sub Regions" per-continent download pages
#     (alps, dach, us-midwest, ...);
#   - "us": every `us/<state>` extract declares `parent: "north-america"`
#     instead of "us", so nothing marks "us" itself as already covered by
#     its states; left in, the whole country would get built twice over
#     (once as "us", once as every individual state);
#   - "enfield", a one-off (the only London borough Geofabrik publishes as
#     its own leaf; see find_leaves() for the other half of that fix).
KNOWN_REDUNDANT_LEAVES = {
    "us", "us-midwest", "us-northeast", "us-pacific", "us-south", "us-west",
    "alps", "britain-and-ireland", "dach", "great-britain",
    "south-africa-and-lesotho", "sea", "kaliningrad",
    "enfield",
}


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
    by_id = {f["properties"]["id"]: f["properties"] for f in features}
    # A redundant leaf (see KNOWN_REDUNDANT_LEAVES, e.g. "enfield") must not
    # count towards making *its own parent* look non-leaf: if it did,
    # excluding "enfield" would leave "greater-london" excluded too (still
    # "referenced as a parent"), so neither ever gets fetched.
    parents_referenced = {
        by_id[gid].get("parent") for gid in by_id
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
    return any(
        path == scope or path.startswith(scope + "/")
        for scope in region_scope.split(",")
    )


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


def queue_path(scope):
    return f"state/queue/{scope}.json"


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
    # docs/ARCHITECTURE.md "Timing history": the two metrics are different
    # units, sorting them as one numeric range would be meaningless).
    # Secondary, within each group: longest/largest first.
    regions.sort(key=lambda r: (not r["has_history"], -r["sort_metric"]))

    # has_history/sort_metric only exist to compute this order; the order
    # itself is what's worth keeping (as array position in the persisted
    # queue, see write_queue), not the metrics that produced it.
    return {
        "region_scope": region_scope,
        "regions": [{"id": r["id"], "pbf_url": r["pbf_url"]} for r in regions],
    }


def write_queue(region_scope, repo, token, state_branch, output_basename, scope):
    """Builds this run's queue (see build_manifest) and seeds
    state/queue/<scope>.json: `remaining` gets the full longest-first
    candidate list, `lock`/`done`/`failed` start empty. This *is* the queue
    from here on, claim.py pops entries straight off `remaining`, and the
    write unconditionally overwrites via update_json_file_with_retry
    rather than a plain create (see docs/ARCHITECTURE.md "Locking" for why
    both are safe here).
    """
    manifest = build_manifest(region_scope, repo, token, state_branch, output_basename)
    content = {"remaining": manifest["regions"], "lock": [], "done": [], "failed": []}
    common.update_json_file_with_retry(
        repo, token, state_branch, queue_path(scope),
        lambda _content: content,
        message=f"seed queue for {scope!r} ({len(content['remaining'])} region(s))",
    )
    return content


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--region-scope", default="",
        help='Geofabrik path prefix, or comma-separated list of prefixes, to restrict the build to (e.g. "europe/monaco" or "europe/monaco,europe/andorra"). Empty means the whole world.',
    )
    parser.add_argument("--repo", required=True, help="owner/repo of the calling repository (state lives on its own 'state' branch)")
    parser.add_argument("--token", required=True)
    parser.add_argument("--state-branch", default="state")
    parser.add_argument("--output-basename", required=True)
    args = parser.parse_args()

    scope = common.sanitize_ref_component(args.output_basename)
    content = write_queue(
        args.region_scope, args.repo, args.token, args.state_branch, args.output_basename, scope,
    )

    print(f"{len(content['remaining'])} region(s) in scope {args.region_scope!r}, written to {queue_path(scope)}", file=sys.stderr)


if __name__ == "__main__":
    main()
