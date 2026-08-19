#!/usr/bin/env python3
"""Reads the run's queue state directly from the calling repository's own
`state` branch. Prints one JSON object
`{"done": [...], "failed": [...], "remaining": [...]}` (region ids) to
stdout. `remaining` is what's neither done nor permanently failed (still in
the queue's own `remaining` list, or still `lock`ed) — a non-empty
`remaining` once the claim-loop round is finished means the run is
incomplete (see docs/ARCHITECTURE.md "Merge"): either the queue never got
claimed in time (a worker exiting cleanly on its own time budget, see
"Worker job shape & the 6-hour limit" — no job ever fails for this case), a
worker crashed mid-build and left its claimed batch's lock entries stuck
(see docs/ARCHITECTURE.md "Locking"), or a worker finished its regions but
its outcome buffer never made it into `flush-timings` (see
cmd_flush_timings in claim.py). This is why `verify-complete` (the job that
runs this) waits on `record-timings` (the job that runs `flush-timings`),
not just on `build`.

With --fail-if-incomplete, exits non-zero (after printing the JSON) when
`remaining` is non-empty, for use as the very last check before merging.
"""

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
