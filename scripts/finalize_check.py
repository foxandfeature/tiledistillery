#!/usr/bin/env python3
"""Compares the run's manifest against current claim-ref state on the
calling repository's own `state` branch. Prints one JSON object
`{"done": [...], "failed": [...], "remaining": [...]}` (region ids) to
stdout. `remaining` is what's neither done nor permanently failed — a
non-empty `remaining` after the top-up rounds are exhausted means the run
is incomplete (see docs/ARCHITECTURE.md "Merge").

With --fail-if-incomplete, exits non-zero (after printing the JSON) when
`remaining` is non-empty, for use as the very last check before merging.
"""

import argparse
import json
import sys

import claim


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--state-branch", default="state")
    parser.add_argument("--output-basename", required=True)
    parser.add_argument("--fail-if-incomplete", action="store_true")
    args = parser.parse_args()

    with open(args.manifest) as f:
        manifest = json.load(f)

    scope = claim.scope_prefix(args.output_basename)
    state = claim.load_state(args.repo, args.token, scope)

    done, failed, remaining = [], [], []
    for region in manifest["regions"]:
        region_id = region["id"]
        region_key = claim.region_key_of(region_id)
        if region_key in state.done:
            done.append(region_id)
        elif region_key in state.failed:
            failed.append(region_id)
        else:
            remaining.append(region_id)

    result = {"done": done, "failed": failed, "remaining": remaining}
    print(json.dumps(result))

    if failed:
        print(f"::warning::{len(failed)} region(s) permanently failed: {', '.join(failed)}", file=sys.stderr)

    if args.fail_if_incomplete and remaining:
        print(f"::error::{len(remaining)} region(s) still not built after all rounds: {', '.join(remaining)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
