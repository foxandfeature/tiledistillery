#!/usr/bin/env python3
"""Claim lifecycle over a shared JSON claims file, against the calling
repository's own `state` branch. See docs/ARCHITECTURE.md "Locking":
`state/claims/<scope>.json` holds three region-key lists — `lock`, `done`,
`failed` — read and written through `common.update_json_file_with_retry`,
the same read-modify-write-with-retry-on-conflict primitive `timings.json`
already used (this replaced an earlier one-git-ref-per-region design; see
ARCHITECTURE.md for why that stopped being reliable).

`next` claims a small *batch* of regions (`--batch-size`, default 5) in one
read-modify-write instead of one API round-trip per region, caching the
rest locally (`--batch-cache`) so a worker only touches the shared file
once every `--batch-size` regions. This is the actual fix for the request
volume that was exhausting the calling workflow's hourly GitHub API quota:
a full-fan-out run over hundreds of regions was making thousands of calls
(a fresh listing plus a ref create/delete per region, times every worker) —
comfortably over budget regardless of how well-tuned the retry/backoff was,
since a *primary* quota exhaustion (GitHub's "API rate limit exceeded for
installation") doesn't clear for up to an hour, unlike the secondary/abuse
limit short backoff is meant for.

A locked region is never reclaimed within the same run — same accepted
tradeoff the old ref design made (see docs/ARCHITECTURE.md "Locking"): a
crashed worker strands up to `--batch-size` regions instead of just one,
which is the one real cost of batching, kept small by keeping the batch
size small. `cleanup_claims.py` wipes the whole scope's state at the end of
every run regardless of how it went, so this can't accumulate across runs.

Subcommands, each printing one JSON object to stdout:

    claim.py next          --manifest queue-manifest.json ...        -> {"claimed": bool, "region": {...}|null}
    claim.py done          --region-id X --duration-seconds N ...
    claim.py failed        --region-id X [--error MSG] ...           -> {"permanently_failed": true}
    claim.py flush-timings --timings-dir DIR ...                     -> {"flushed": int}

`failed` is a single strike, not a retry counter: a region either finishes
via `done` or is marked permanently `failed` the first time
`claim-and-build` calls this after a build fails. Retrying transient
failures (a flaky download, a flaky GitHub API call) is the caller's job,
on the same runner, *before* it ever calls `failed` — see the retry
flags/loops on `claim-and-build`'s own `curl` step and `common.py`'s
`_request` — since retrying a genuinely broken region (bad Geofabrik data,
a real 404, or a build that fails on tilemaker's own terms) from here would
just waste other workers' time on the same failure. Deliberately not
retried here: a `docker run tilemaker` failure. Unlike a download or a
GitHub API call, there's no *server* on the other end of it that could be
transiently down — this runner is the only thing running it — so a nonzero
exit is tilemaker's own verdict on this region's data, not a blip worth
retrying.

`done` and `failed` don't touch the shared claims file at all: both append
an outcome line to `--timings-buffer` locally (same buffer file `done`
always used for durations — it now carries a `status` per line, not just a
duration). `flush-timings` is run once, by a single dedicated job, against
every worker's uploaded buffer (see _pipeline.yml's `record-timings` job) —
one writer, one write, for both the timing-history file and the shared
claims file's `done`/`failed` lists. See cmd_done's docstring for why
per-worker/per-region writes from inside the claim loop are the thing being
avoided.
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


def claims_path(scope):
    return f"state/claims/{scope}.json"


class ClaimState:
    """Parsed view of one scope's claims file (`{"lock": [...], "done":
    [...], "failed": [...]}`, each a list of region keys).

    `lock` is the actual mutex: a region key only ever enters it once, via
    the compare-and-swap `_claim_batch` does inside
    `update_json_file_with_retry`'s retry-on-conflict loop, so two workers
    racing the same read-modify-write can't both add the same key (whichever
    write lands second re-reads the first one's already-updated `lock` and
    skips it). There is no lease/timestamp and no staleness sweep: a lock is
    only ever released by `flush-timings` moving the key into `done`/`failed`
    once the worker that claimed it reports back. A worker that never gets
    there (crashed, OOM-killed, hit the job time limit) leaves its claimed
    batch's remaining region(s) stuck for the rest of this run — the same
    tradeoff the earlier one-ref-per-region design made, just with a blast
    radius of `--batch-size` regions instead of one; `cleanup_claims.py`
    wipes everything at the end of every run regardless of how it went, so
    this can't accumulate across runs. `worker_count` (see
    docs/ARCHITECTURE.md "Distribution") is the mitigation: enough workers
    fanned out into one round means one stuck batch costs a handful of
    regions, not the run, and every other worker keeps draining the queue
    regardless.
    """

    def __init__(self, content):
        self.lock = set(content.get("lock", []))
        self.done = set(content.get("done", []))
        self.failed = set(content.get("failed", []))

    def unavailable(self, region_key):
        return region_key in self.done or region_key in self.failed or region_key in self.lock


def load_state(repo, token, state_branch, scope):
    content, _ = common.get_json_file(repo, token, state_branch, claims_path(scope))
    return ClaimState(content)


def region_key_of(region_id):
    return "/".join(common.sanitize_ref_component(p) for p in region_id.split("/"))


# Small on purpose: this bounds how many regions one crashed/OOM-killed/
# timed-out worker can strand for the rest of a run (see ClaimState's
# docstring) while still cutting the shared-file round trips claiming
# "hundreds of leaves" (docs/ARCHITECTURE.md "Distribution") makes by
# roughly this factor — 5 regions per read-modify-write instead of one full
# ref listing plus a create per region is already most of the win; going
# much bigger buys little further request-volume reduction against a much
# worse crash blast radius.
_DEFAULT_BATCH_SIZE = 5


def _load_batch_cache(path):
    p = pathlib.Path(path)
    if not p.exists():
        return []
    with open(p) as f:
        return json.load(f)


def _save_batch_cache(path, regions):
    with open(path, "w") as f:
        json.dump(regions, f)


def _claim_batch(repo, token, state_branch, scope, manifest, batch_size):
    """One read-modify-write claims up to `batch_size` regions at once —
    see the module docstring for why this, not one round trip per region,
    is the actual fix for the request volume that was exhausting the
    calling workflow's hourly GitHub API quota. `update_json_file_with_retry`
    already retries the whole read-modify-write on a conflicting concurrent
    write from another worker, re-picking a fresh batch against the
    re-read state each attempt, so two workers racing this can't end up
    with an overlapping batch."""
    claimed = []

    def mutate(content):
        claimed.clear()
        state = ClaimState(content)
        locked = set(state.lock)
        for region in manifest["regions"]:
            if len(claimed) >= batch_size:
                break
            region_key = region_key_of(region["id"])
            if region_key in locked or state.unavailable(region_key):
                continue
            claimed.append(region)
            locked.add(region_key)
        content["lock"] = sorted(locked)
        return content

    common.update_json_file_with_retry(
        repo, token, state_branch, claims_path(scope), mutate,
        message=f"claim up to {batch_size} region(s) for {scope!r}",
    )
    return claimed


def cmd_next(args):
    batch = _load_batch_cache(args.batch_cache)
    if not batch:
        # ensure_branch is itself an API call (a GET, plus a POST the very
        # first time ever) — only paid when the cache is actually empty and
        # a real batch claim is about to happen, not on every cache-served
        # call, or it would silently undo most of batching's point.
        common.ensure_branch(args.repo, args.token, args.state_branch)
        with open(args.manifest) as f:
            manifest = json.load(f)
        scope = scope_prefix(args.output_basename)
        try:
            batch = _claim_batch(args.repo, args.token, args.state_branch, scope, manifest, args.batch_size)
        except common.TokenPermissionError:
            raise
        except (requests.RequestException, RuntimeError) as exc:
            # A persistent GitHub outage (5xx/network trouble surviving
            # common._request's own retries, or update_json_file_with_retry
            # giving up after its own retry-on-conflict attempts) shouldn't
            # crash this worker — nothing was claimed here, so report the
            # same as an empty queue, the same "log and continue" treatment
            # cmd_done/cmd_failed give this class of failure.
            print(f"::warning::claim batch for {args.output_basename} failed: {exc}", file=sys.stderr)
            batch = []

    if not batch:
        _save_batch_cache(args.batch_cache, [])
        print(json.dumps({"claimed": False, "region": None}))
        return

    region = batch.pop(0)
    _save_batch_cache(args.batch_cache, batch)
    print(json.dumps({"claimed": True, "region": region}))


def cmd_done(args):
    # No API call at all: outcome goes to a local, per-worker file, not
    # straight to the shared claims file — a worker claims tens of regions
    # in a row, and writing the shared file here on every single `done`
    # would turn it into a hot lock (`worker_count` workers finishing
    # regions close together all racing the same read-modify-write, which
    # under a full fan-out can exhaust update_json_file_with_retry's
    # attempts outright — see docs/ARCHITECTURE.md "Timing history & queue
    # ordering" for why this exact pattern was already avoided for
    # durations). Appending to a local file is a pure filesystem write — no
    # network round trip, so it can't collide with anything, and can't fail
    # the way the old create_ref/delete_ref pair here could. cmd_flush_timings
    # merges every worker's buffer into the shared claims and timing-history
    # files in one write each per run; see its docstring.
    with open(args.timings_buffer, "a") as f:
        f.write(json.dumps({
            "region_id": args.region_id,
            "status": "done",
            "duration_seconds": args.duration_seconds,
        }) + "\n")

    print(json.dumps({"ok": True}))


def cmd_flush_timings(args):
    """Merges every worker's locally buffered outcomes (from repeated
    cmd_done/cmd_failed calls, one JSON line each, uploaded as one artifact
    per worker) into the shared timing-history file and the shared claims
    file's `done`/`failed` lists, one read-modify-write each, for the whole
    run — called exactly once, by a dedicated job downstream of `build`
    (see `record-timings` in _pipeline.yml). This is the actual fix for the
    contention cmd_done's docstring describes: not a smaller worker-sized
    batch racing other workers' batches, but a single writer, so there is
    no concurrent writer left to race at all. It's also what finally
    releases each region's `lock` entry (see ClaimState's docstring) —
    unlike the old design, a region isn't durably done/failed until this
    runs, so a worker that dies before its buffer artifact uploads loses
    that outcome the same way it would lose an unuploaded shard file.

    `timings_dir` is searched recursively for buffer files (each worker's
    artifact lands in its own subdirectory after download-artifact, so
    filenames don't need to be unique across workers). Missing or empty
    directory means nothing was buffered (e.g. a run that claimed zero
    regions) — a no-op, not an error.

    Still wrapped, not raised, on failure: this being a single write per
    run makes update_json_file_with_retry's attempts being exhausted far
    less likely than under per-region or per-worker writes, but GitHub API
    trouble unrelated to write contention (an outage, a persistent 5xx)
    is still possible. Losing this run's timing sample is fine — regions
    with no recorded history just fall back to byte-size ordering (see
    docs/ARCHITECTURE.md "Timing history & queue ordering"). Losing the
    claims flush is not fine for this run's completeness (those regions
    stay `remaining` and `finalize_check.py --fail-if-incomplete` fails the
    run below), but is still not raised here — this job runs unconditionally
    and independently of `verify-complete` precisely so one failed `build`
    leg's timing data isn't lost over it; a warning is enough."""
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

    done_entries = [e for e in entries if e.get("status") == "done"]
    failed_entries = [e for e in entries if e.get("status") == "failed"]

    if done_entries:
        def apply_timings(timings):
            for entry in done_entries:
                history = timings.get(entry["region_id"], [])
                history = (history + [entry["duration_seconds"]])[-5:]
                timings[entry["region_id"]] = history
            return timings

        try:
            common.update_json_file_with_retry(
                args.repo, args.token, args.state_branch,
                leaves.timings_path(args.output_basename),
                apply_timings,
                message=f"record timing for {len(done_entries)} region(s)",
            )
        except RuntimeError as exc:
            print(f"::warning::timing history flush failed for {len(done_entries)} region(s): {exc}", file=sys.stderr)

    scope = scope_prefix(args.output_basename)

    def apply_claims(content):
        state = ClaimState(content)
        lock, done, failed = set(state.lock), set(state.done), set(state.failed)
        for entry in done_entries:
            key = region_key_of(entry["region_id"])
            lock.discard(key)
            done.add(key)
        for entry in failed_entries:
            key = region_key_of(entry["region_id"])
            lock.discard(key)
            failed.add(key)
        content["lock"] = sorted(lock)
        content["done"] = sorted(done)
        content["failed"] = sorted(failed)
        return content

    try:
        common.update_json_file_with_retry(
            args.repo, args.token, args.state_branch, claims_path(scope), apply_claims,
            message=f"record {len(done_entries)} done, {len(failed_entries)} failed region(s) for {scope!r}",
        )
    except RuntimeError as exc:
        print(f"::warning::claims flush failed for {len(entries)} region(s): {exc}", file=sys.stderr)

    print(json.dumps({"flushed": len(entries)}))


def cmd_failed(args):
    # See cmd_done's docstring: same "no API call, buffer locally" reasoning
    # applies here.
    with open(args.timings_buffer, "a") as f:
        f.write(json.dumps({
            "region_id": args.region_id,
            "status": "failed",
            "error": args.error,
        }) + "\n")
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
    p_next.add_argument("--batch-size", type=int, default=_DEFAULT_BATCH_SIZE, help="Regions to claim per shared-file read-modify-write; see _DEFAULT_BATCH_SIZE.")
    p_next.add_argument("--batch-cache", required=True, help="Local file this worker's still-unclaimed batch is cached in between calls, so only every Nth `next` call touches the API.")
    p_next.set_defaults(func=cmd_next)

    p_done = sub.add_parser("done")
    _add_common_args(p_done)
    p_done.add_argument("--region-id", required=True)
    p_done.add_argument("--duration-seconds", type=int, required=True)
    p_done.add_argument("--timings-buffer", required=True, help="Local file this worker's outcomes are appended to; see cmd_flush_timings.")
    p_done.set_defaults(func=cmd_done)

    p_failed = sub.add_parser("failed")
    _add_common_args(p_failed)
    p_failed.add_argument("--region-id", required=True)
    p_failed.add_argument("--error", default="")
    p_failed.add_argument("--timings-buffer", required=True, help="Local file this worker's outcomes are appended to; see cmd_flush_timings.")
    p_failed.set_defaults(func=cmd_failed)

    p_flush = sub.add_parser("flush-timings")
    _add_common_args(p_flush)
    p_flush.add_argument("--timings-dir", required=True, help="Directory to search recursively for workers' uploaded timing-buffer files.")
    p_flush.set_defaults(func=cmd_flush_timings)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
