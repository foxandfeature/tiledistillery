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

    claim.py next          --round build|topup --manifest queue-manifest.json ...  -> {"claimed": bool, "region": {...}|null}
    claim.py done          --round build|topup --region-id X --duration-seconds N ...
    claim.py failed        --round build|topup --region-id X [--error MSG] ...     -> {"permanently_failed": true}
    claim.py flush-timings --timings-dir DIR ...                                   -> {"flushed": int}

`--round` (`next`/`done`/`failed` only) says which of `_pipeline.yml`'s two
claim-loop rounds is calling — see ClaimState's docstring for why the lock
ref itself is scoped by round.

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

import requests

import common
import leaves


def scope_prefix(output_basename):
    return common.sanitize_ref_component(output_basename)


class ClaimState:
    """Parsed view of every ref under one scope prefix.

    `.../lock/<round>` is a *fixed* name per region **per round** ("build"
    or "topup", see `_pipeline.yml`'s two claim-loop rounds) — the actual
    mutex, since two workers racing for the same region *in the same round*
    only collide (one 422s) if they're trying to create the exact same ref
    name. Rounds run strictly sequentially — `topup` only starts once every
    `build`-round job has already terminated, one way or another, per the
    workflow's own `needs:` graph — so a `lock/build` ref still present when
    `topup` starts can only mean its worker never reached `done`/`failed`
    (crashed, OOM-killed, hit the job time limit) and is *guaranteed* dead by
    now, not a live worker `topup` could race. Scoping the lock name by round
    is what lets `topup` treat that as reclaimable without needing a
    timestamp or any staleness sweep: it simply never looks at `lock/build`
    at all, only `lock/topup`. There is still no companion timestamp ref
    within a round — two workers in the *same* round remain a real race,
    settled by the atomic ref-create exactly as before. `cleanup_claims.py`
    wipes everything at the end of every run regardless of how it went, so a
    stuck `lock/<round>` can't accumulate across runs either way.
    """

    def __init__(self, refs, prefix):
        self.lock_present = {}  # region_id -> set of round names
        self.done = set()
        self.failed = set()
        for full_ref in refs:
            if not full_ref.startswith(prefix):
                continue
            self._parse(full_ref[len(prefix):])

    def _parse(self, remainder):
        # remainder is "<region/id/path>/lock/<round>" | ".../done" | ".../failed"
        if remainder.endswith("/done"):
            self.done.add(remainder[: -len("/done")])
            return
        if remainder.endswith("/failed"):
            self.failed.add(remainder[: -len("/failed")])
            return
        segments = remainder.split("/")
        if len(segments) >= 2 and segments[-2] == "lock":
            region_id = "/".join(segments[:-2])
            self.lock_present.setdefault(region_id, set()).add(segments[-1])

    def unavailable(self, region_id, round_name):
        return (
            region_id in self.done
            or region_id in self.failed
            or round_name in self.lock_present.get(region_id, ())
        )


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
        if state.unavailable(region_key, args.round):
            continue
        # The actual race: a *fixed* ref name per region per round, so two
        # workers racing for the same region in the same round collide on
        # this exact create — only one can win (see ClaimState's docstring
        # for why this must not have a timestamp or any other varying
        # component in it, and why the round suffix is fine).
        if not common.create_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock/{args.round}", anchor_sha):
            continue  # lost the race for this one; try the next candidate

        print(json.dumps({"claimed": True, "region": region}))
        return

    print(json.dumps({"claimed": False, "region": None}))


def cmd_done(args):
    anchor_sha = common.ensure_branch(args.repo, args.token, args.state_branch)
    scope = scope_prefix(args.output_basename)
    region_key = region_key_of(args.region_id)

    # Not raised on failure: a persistent GitHub outage (5xx/network trouble
    # surviving common._request's own retries) landing here means the region
    # already finished building — crashing this worker over it would abort
    # its whole claim loop and strand every region still left in its queue,
    # not just this one. Per ClaimState's docstring, a lock nothing ever
    # releases is already an accepted state (indistinguishable from a worker
    # that crashed outright) that cleanup_claims.py wipes at the end of the
    # run regardless, so leaving it stuck here is strictly better than
    # taking the whole worker down with it. TokenPermissionError (a real
    # permissions problem, not a blip) is deliberately not caught here — see
    # its docstring — and still propagates.
    try:
        common.create_ref(args.repo, args.token, f"claims/{scope}/{region_key}/done", anchor_sha)
        common.delete_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock/{args.round}")
    except requests.RequestException as exc:
        print(f"::warning::{args.region_id} done-marking failed, lock left stuck for this run: {exc}", file=sys.stderr)

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

    # See cmd_done's matching try/except: same reasoning applies here, so a
    # transient GitHub outage strands one lock instead of this worker's
    # whole remaining queue.
    try:
        common.create_ref(args.repo, args.token, f"claims/{scope}/{region_key}/failed", anchor_sha)
        common.delete_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock/{args.round}")
    except requests.RequestException as exc:
        print(f"::warning::{args.region_id} failed-marking failed, lock left stuck for this run: {exc}", file=sys.stderr)
    if args.error:
        print(f"::warning::{args.region_id} failed: {args.error}", file=sys.stderr)
    print(json.dumps({"permanently_failed": True}))


def _add_common_args(p):
    p.add_argument("--repo", required=True, help="owner/repo of the calling repository")
    p.add_argument("--token", required=True)
    p.add_argument("--state-branch", default="state")
    p.add_argument("--output-basename", required=True)


def _add_round_arg(p):
    p.add_argument(
        "--round", required=True, choices=["build", "topup"],
        help="Which claim-loop round this is (see _pipeline.yml). Scopes the lock "
             "ref name so topup can reclaim a region whose build-round lock was "
             "never released — see ClaimState's docstring.",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_next = sub.add_parser("next")
    _add_common_args(p_next)
    _add_round_arg(p_next)
    p_next.add_argument("--manifest", required=True)
    p_next.set_defaults(func=cmd_next)

    p_done = sub.add_parser("done")
    _add_common_args(p_done)
    _add_round_arg(p_done)
    p_done.add_argument("--region-id", required=True)
    p_done.add_argument("--duration-seconds", type=int, required=True)
    p_done.add_argument("--timings-buffer", required=True, help="Local file this worker's durations are appended to; see cmd_flush_timings.")
    p_done.set_defaults(func=cmd_done)

    p_failed = sub.add_parser("failed")
    _add_common_args(p_failed)
    _add_round_arg(p_failed)
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
