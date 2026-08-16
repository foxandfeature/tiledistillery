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

    claim.py next   --manifest queue-manifest.json ...   -> {"claimed": bool, "region": {...}|null}
    claim.py done   --region-id X --duration-seconds N ...
    claim.py failed --region-id X [--error MSG] ...       -> {"permanently_failed": bool, "attempt": int}
"""

import argparse
import json
import sys
import time

import common
import leaves


def scope_prefix(output_basename):
    return common.sanitize_ref_component(output_basename)


class ClaimState:
    """Parsed view of every ref under one scope prefix.

    Two refs per in-progress claim, not one: `.../lock` is a *fixed* name
    per region — the actual mutex, since two workers racing for the same
    region only collide (one 422s) if they're trying to create the exact
    same ref name. `.../lock-at/<ts>` is a companion created immediately
    after by whoever won `.../lock`, purely so a later sweep can tell how
    old a claim is without reading any ref's pointed-to commit content —
    it's never itself contested, since only the winner of `.../lock` ever
    gets far enough to create it. (An earlier version put the timestamp
    directly in the lock ref's own name, e.g. `.../lock/<ts>` — which
    silently broke mutual exclusion: two workers claiming the same region
    in different seconds would each create a *different*, non-colliding
    ref name and both believe they'd won.)
    """

    def __init__(self, refs, prefix):
        self.lock_present = set()  # region_id
        self.lock_at = {}          # region_id -> timestamp (best-effort, see sweep_stale)
        self.done = set()
        self.failed = set()
        self.attempts = {}         # region_id -> set of attempt numbers
        for full_ref in refs:
            if not full_ref.startswith(prefix):
                continue
            self._parse(full_ref[len(prefix):])

    def _parse(self, remainder):
        # remainder is "<region/id/path>/lock" | ".../lock-at/<ts>" | ".../done" | ".../attempt/<n>" | ".../failed"
        if remainder.endswith("/done"):
            self.done.add(remainder[: -len("/done")])
            return
        if remainder.endswith("/failed"):
            self.failed.add(remainder[: -len("/failed")])
            return
        if remainder.endswith("/lock"):
            self.lock_present.add(remainder[: -len("/lock")])
            return
        if "/lock-at/" in remainder:
            region, _, ts = remainder.rpartition("/lock-at/")
            self.lock_at[region] = int(ts)
            return
        if "/attempt/" in remainder:
            region, _, n = remainder.rpartition("/attempt/")
            self.attempts.setdefault(region, set()).add(int(n))
            return

    def unavailable(self, region_id):
        return region_id in self.done or region_id in self.failed or region_id in self.lock_present

    def attempt_count(self, region_id):
        return len(self.attempts.get(region_id, ()))


MAX_ATTEMPTS = 3


def load_state(repo, token, scope):
    refs = common.list_matching_refs(repo, token, f"claims/{scope}")
    return ClaimState(refs, prefix=f"refs/claims/{scope}/")


def region_key_of(region_id):
    return "/".join(common.sanitize_ref_component(p) for p in region_id.split("/"))


def sweep_stale(repo, token, scope, state, ttl_seconds, now, anchor_sha):
    """Releases locks older than the TTL and records the attempt, so a
    crashed worker's claim becomes reclaimable without a separate job. A
    lock with no `lock-at` ref at all (a crash landed between creating the
    two) is treated as stale immediately — there's no age to compare, and
    leaving it locked forever would be worse than an extra attempt.

    One region's release is skipped, not raised, on an unexpected failure
    (for example a persistent GitHub API error): the lock ref for that
    region is left untouched, so a later sweep just retries it. Raising
    here would abort the whole caller (`next`/`failed`), which crashes
    that worker's claim loop entirely and leaves every other stale lock in
    this batch unreleased too, and leaves this region's own lock stuck
    forever since the delete never even runs."""
    for region_key in list(state.lock_present):
        ts = state.lock_at.get(region_key)
        if ts is not None and now - ts <= ttl_seconds:
            continue
        try:
            _release_and_record_attempt(repo, token, scope, region_key, state, anchor_sha)
        except Exception as exc:
            print(f"::warning::failed to sweep stale claim for {region_key}: {exc}", file=sys.stderr)


def _release_and_record_attempt(repo, token, scope, region_key, state, anchor_sha):
    n = state.attempt_count(region_key) + 1
    common.create_ref(repo, token, f"claims/{scope}/{region_key}/attempt/{n}", anchor_sha)
    common.delete_ref(repo, token, f"claims/{scope}/{region_key}/lock")
    ts = state.lock_at.pop(region_key, None)
    if ts is not None:
        common.delete_ref(repo, token, f"claims/{scope}/{region_key}/lock-at/{ts}")
    state.lock_present.discard(region_key)
    state.attempts.setdefault(region_key, set()).add(n)
    if n >= MAX_ATTEMPTS:
        common.create_ref(repo, token, f"claims/{scope}/{region_key}/failed", anchor_sha)
        state.failed.add(region_key)


def cmd_next(args):
    anchor_sha = common.ensure_branch(args.repo, args.token, args.state_branch)

    with open(args.manifest) as f:
        manifest = json.load(f)

    scope = scope_prefix(args.output_basename)
    state = load_state(args.repo, args.token, scope)
    sweep_stale(args.repo, args.token, scope, state, args.ttl_seconds, int(time.time()), anchor_sha)

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

        # Having won, record *when* purely for staleness bookkeeping later.
        # No race here: only the winner of the create above ever reaches
        # this line, so this create can't collide with anything.
        common.create_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock-at/{int(time.time())}", anchor_sha)
        print(json.dumps({"claimed": True, "region": region}))
        return

    print(json.dumps({"claimed": False, "region": None}))


def cmd_done(args):
    anchor_sha = common.ensure_branch(args.repo, args.token, args.state_branch)
    scope = scope_prefix(args.output_basename)
    region_key = region_key_of(args.region_id)
    state = load_state(args.repo, args.token, scope)

    common.create_ref(args.repo, args.token, f"claims/{scope}/{region_key}/done", anchor_sha)
    common.delete_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock")
    ts = state.lock_at.get(region_key)
    if ts is not None:
        common.delete_ref(args.repo, args.token, f"claims/{scope}/{region_key}/lock-at/{ts}")

    def append_duration(timings):
        history = timings.get(args.region_id, [])
        history = (history + [args.duration_seconds])[-5:]
        timings[args.region_id] = history
        return timings

    common.update_json_file_with_retry(
        args.repo, args.token, args.state_branch,
        leaves.timings_path(args.output_basename),
        append_duration,
        message=f"record timing for {args.region_id}",
    )
    print(json.dumps({"ok": True}))


def cmd_failed(args):
    anchor_sha = common.ensure_branch(args.repo, args.token, args.state_branch)
    scope = scope_prefix(args.output_basename)
    state = load_state(args.repo, args.token, scope)
    region_key = region_key_of(args.region_id)

    _release_and_record_attempt(args.repo, args.token, scope, region_key, state, anchor_sha)
    n = state.attempt_count(region_key)
    if args.error:
        print(f"::warning::{args.region_id} attempt {n} failed: {args.error}", file=sys.stderr)
    print(json.dumps({"permanently_failed": region_key in state.failed, "attempt": n}))


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
    p_next.add_argument("--ttl-seconds", type=int, default=5400)
    p_next.set_defaults(func=cmd_next)

    p_done = sub.add_parser("done")
    _add_common_args(p_done)
    p_done.add_argument("--region-id", required=True)
    p_done.add_argument("--duration-seconds", type=int, required=True)
    p_done.set_defaults(func=cmd_done)

    p_failed = sub.add_parser("failed")
    _add_common_args(p_failed)
    p_failed.add_argument("--region-id", required=True)
    p_failed.add_argument("--error", default="")
    p_failed.set_defaults(func=cmd_failed)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
