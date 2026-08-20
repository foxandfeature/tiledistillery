#!/usr/bin/env python3
"""Reads the run's queue state from the calling repo's `state` branch.
Prints `{"done": [...], "failed": [...], "remaining": [...]}` (region ids).
`remaining` (still in the queue's own list, or still `lock`ed) being
non-empty once the claim-loop round is finished means the run is
incomplete (docs/ARCHITECTURE.md "Merge"): a worker exiting on its own time
budget, a crash leaving `lock` entries stuck (see "Locking"), or a worker's
outcome buffer never reaching `flush-timings`. That last case is why
`verify-complete` (the job that runs this) waits on `record-timings`, not
just `build`.

With --fail-if-incomplete, exits non-zero when `remaining` is non-empty,
for use as the last check before merging."""

import argparse
import json
import sys

import claim


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--state-branch", default="state")
    parser.add_argument("--output-basename", required=True)
    parser.add_argument("--fail-if-incomplete", action="store_true")
    args = parser.parse_args()

    scope = claim.scope_prefix(args.output_basename)
    state = claim.load_state(args.repo, args.token, args.state_branch, scope)

    done = sorted(state.done)
    failed = sorted(state.failed)
    remaining = sorted({r["id"] for r in state.remaining} | state.lock)

    result = {"done": done, "failed": failed, "remaining": remaining}
    print(json.dumps(result))

    if failed:
        print(f"::warning::{len(failed)} region(s) permanently failed: {', '.join(failed)}", file=sys.stderr)

    if args.fail_if_incomplete and remaining:
        print(f"::error::{len(remaining)} region(s) still not built: {', '.join(remaining)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
