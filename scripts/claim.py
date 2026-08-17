#!/usr/bin/env python3
"""Claim lifecycle over the git-ref locking scheme, against the calling
repository's own `state` branch. See docs/ARCHITECTURE.md "Locking": one
region's claim state lives entirely in ref *names* under
`refs/claims/<output_basename>/<region-id>/...`, so a single ref listing is
enough to know what's claimed, done, or failed — no ref ever needs its
pointed-to commit's content read back. `done`/`failed` re-derive which
lock ref(s) to release themselves (one fresh listing each) rather than
having the caller thread a ref path through the CLI, since region-id alone
is always enough to reconstruct it.

Subcommands, each printing one JSON object to stdout:

    claim.py next          --manifest queue-manifest.json ...        -> {"claimed": bool, "region": {...}|null}
    claim.py done          --region-id X --duration-seconds N ...
    claim.py failed        --region-id X [--error MSG] ...           -> {"permanently_failed": true}
    claim.py flush-timings --timings-dir DIR ...                     -> {"flushed": int}

`failed` is a single strike, not a retry counter: a region either finishes
via `done` or is marked permanently `failed` the first time
`claim-and-build` calls this after a build fails. Retrying transient
failures (a flaky download) is the caller's job, on the same runner,
*before* it ever calls `failed` — see the retry flags on `claim-and-build`'s
own `curl` step — since retrying a genuinely broken region (bad Geofabrik
data, a real 404) from here would just waste other workers' time on the
same failure.

`done` buffers its duration locally (--timings-buffer) rather than writing
the shared timing-history file itself. `flush-timings` is run once, by a
single dedicated job, against every worker's uploaded buffer from both
rounds (see _pipeline.yml's `record-timings` job) — one writer, one write,
for the whole run. See cmd_done's docstring for why per-worker/per-region
writes from inside the claim loop are the thing being avoided.
"""

import argparse
import json
import pathlib
import sys

import common
import leaves


def scope_prefix(output_basename):
    return common.sanitize_ref_component(output_basename)


class ClaimState:
    """Parsed view of every ref under one scope prefix.

    `.../lock` is a *fixed* name per region — the actual mutex, since two
    workers racing for the same region only collide (one 422s) if they're
    trying to create the exact same ref name. There is no companion
    timestamp ref and no staleness sweep: a lock is only ever released by
    the worker that holds it, via `done` or `failed`. A worker that never
    gets there (crashed, OOM-killed, hit the job time limit) leaves its
    region's `lock` stuck for the rest of this run — accepted for now to
    keep the state machine to three ref kinds; `cleanup_claims.py` wipes
    everything at the end of every run regardless of how it went, so this
    can't accumulate across runs.
    """

    def __init__(self, refs, prefix):
        self.lock_present = set()  # region_id
        self.done = set()
        self.failed = set()
        for full_ref in refs:
            if not full_ref.startswith(prefix):
                continue
            self._parse(full_ref[len(prefix):])

    def _parse(self, remainder):
        # remainder is "<region/id/path>/lock" | ".../done" | ".../failed"
        if remainder.endswith("/done"):
            self.done.add(remainder[: -len("/done")])
            return
        if remainder.endswith("/failed"):
            self.failed.add(remainder[: -len("/failed")])
            return
        if remainder.endswith("/lock"):
            self.lock_present.add(remainder[: -len("/lock")])
            return

    def unavailable(self, region_id):
        return region_id in self.done or region_id in self.failed or region_id in self.lock_present


def load_state(repo, token, scope):
    refs = common.list_matching_refs(repo, token, f"claims/{scope}")
    return ClaimState(refs, prefix=f"refs/claims/{scope}/")


def region_key_of(region_id):
    return "/".join(common.sanitize_ref_component(p) for p in region_id.split("/"))


def cmd_next(args):
    anchor_sha = common.ensure_branch(args.repo, args.token, args.state_branch)

    with open(args.manifest) as f:
        manifest = json.load(f)

    scope = scope_prefix(args.output_basename)
    state = load_state(args.repo, args.token, scope)

    for region in manifest["regions"]:
        region_key = region_key_of(region["id"])
        if state.unavailable(region_key):
            continue
        # The actual race: a *fixed* ref name per region, so two workers
        # racing for the same region collide on this exact create — only
        # one can win (see ClaimState's docstring for why this must not
        # have a timestamp or any other varying component in it).
        if not common.create_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock", anchor_sha):
            continue  # lost the race for this one; try the next candidate

        print(json.dumps({"claimed": True, "region": region}))
        return

    print(json.dumps({"claimed": False, "region": None}))


def cmd_done(args):
    anchor_sha = common.ensure_branch(args.repo, args.token, args.state_branch)
    scope = scope_prefix(args.output_basename)
    region_key = region_key_of(args.region_id)

    common.create_ref(args.repo, args.token, f"claims/{scope}/{region_key}/done", anchor_sha)
    common.delete_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock")

    # Duration goes to a local, per-worker file, not straight to the shared
    # state/timings/<output_basename>.json — a worker claims tens of regions
    # in a row, and this shared file is the one piece of per-region state
    # *not* keyed by its own ref (unlike the claim/done/failed refs above),
    # so writing it here on every single `done` turns it into a hot lock:
    # `worker_count` workers finishing regions close together all race the
    # same read-modify-write, and under a full fan-out that can exhaust
    # update_json_file_with_retry's attempts outright. Appending to a local
    # file is a pure filesystem write — no network round trip, so it can't
    # collide with anything. cmd_flush_timings merges the whole buffer in
    # one shared-file write per worker instead of one per region; see its
    # docstring.
    with open(args.timings_buffer, "a") as f:
        f.write(json.dumps({"region_id": args.region_id, "duration_seconds": args.duration_seconds}) + "\n")

    print(json.dumps({"ok": True}))


def cmd_flush_timings(args):
    """Merges every worker's locally buffered durations (from repeated
    cmd_done calls, one JSON line each, uploaded as one artifact per worker
    per round) into the shared timing-history file in a single
    read-modify-write for the whole run — called exactly once, by a
    dedicated job downstream of both the build and topup rounds (see
    `record-timings` in _pipeline.yml). This is the actual fix for the
    contention cmd_done's docstring describes: not a smaller worker-sized
    batch racing other workers' batches, but a single writer, so there is
    no concurrent writer left to race at all.

    `timings_dir` is searched recursively for buffer files (each worker's
    artifact lands in its own subdirectory after download-artifact, so
    filenames don't need to be unique across workers). Missing or empty
    directory means nothing was buffered (e.g. a round that claimed zero
    regions) — a no-op, not an error.

    Still wrapped, not raised, on failure: this being a single write per
    run makes update_json_file_with_retry's attempts being exhausted far
    less likely than under per-region or per-worker writes, but GitHub API
    trouble unrelated to write contention (an outage, a persistent 5xx)
    is still possible, and losing this run's timing sample is fine —
    regions with no recorded history just fall back to byte-size ordering
    (see docs/ARCHITECTURE.md "Timing history & queue ordering")."""
    entries = []
    if args.timings_dir and pathlib.Path(args.timings_dir).is_dir():
        for path in sorted(pathlib.Path(args.timings_dir).rglob("*")):
            if not path.is_file():
                continue
            with open(path) as f:
                entries.extend(json.loads(line) for line in f if line.strip())

    if not entries:
        print(json.dumps({"flushed": 0}))
        return

    def apply_batch(timings):
        for entry in entries:
            history = timings.get(entry["region_id"], [])
            history = (history + [entry["duration_seconds"]])[-5:]
            timings[entry["region_id"]] = history
        return timings

    try:
        common.update_json_file_with_retry(
            args.repo, args.token, args.state_branch,
            leaves.timings_path(args.output_basename),
            apply_batch,
            message=f"record timing for {len(entries)} region(s)",
        )
    except RuntimeError as exc:
        print(f"::warning::timing history flush failed for {len(entries)} region(s): {exc}", file=sys.stderr)
    print(json.dumps({"flushed": len(entries)}))


def cmd_failed(args):
    anchor_sha = common.ensure_branch(args.repo, args.token, args.state_branch)
    scope = scope_prefix(args.output_basename)
    region_key = region_key_of(args.region_id)

    common.create_ref(args.repo, args.token, f"claims/{scope}/{region_key}/failed", anchor_sha)
    common.delete_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock")
    if args.error:
        print(f"::warning::{args.region_id} failed: {args.error}", file=sys.stderr)
    print(json.dumps({"permanently_failed": True}))


def _add_common_args(p):
    p.add_argument("--repo", required=True, help="owner/repo of the calling repository")
    p.add_argument("--token", required=True)
    p.add_argument("--state-branch", default="state")
    p.add_argument("--output-basename", required=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_next = sub.add_parser("next")
    _add_common_args(p_next)
    p_next.add_argument("--manifest", required=True)
    p_next.set_defaults(func=cmd_next)

    p_done = sub.add_parser("done")
    _add_common_args(p_done)
    p_done.add_argument("--region-id", required=True)
    p_done.add_argument("--duration-seconds", type=int, required=True)
    p_done.add_argument("--timings-buffer", required=True, help="Local file this worker's durations are appended to; see cmd_flush_timings.")
    p_done.set_defaults(func=cmd_done)

    p_failed = sub.add_parser("failed")
    _add_common_args(p_failed)
    p_failed.add_argument("--region-id", required=True)
    p_failed.add_argument("--error", default="")
    p_failed.set_defaults(func=cmd_failed)

    p_flush = sub.add_parser("flush-timings")
    _add_common_args(p_flush)
    p_flush.add_argument("--timings-dir", required=True, help="Directory to search recursively for workers' uploaded timing-buffer files.")
    p_flush.set_defaults(func=cmd_flush_timings)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
