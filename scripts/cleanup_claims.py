#!/usr/bin/env python3
"""Resets this run's queue scope to empty on the calling repo's `state`
branch, run with `if: always()` at the end of `_pipeline.yml` so the next
run starts clean (the queue is deliberately not run-ID-scoped, see
docs/ARCHITECTURE.md "Locking"). One read plus one read-modify-write, down
from the old per-ref listing+DELETE design. Best effort: a failure is
logged and left for a later cleanup run to retry, not aborted."""

import argparse
import sys

import claim
import common
import leaves


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--state-branch", default="state")
    parser.add_argument("--output-basename", required=True)
    args = parser.parse_args()

    scope = claim.scope_prefix(args.output_basename)
    path = leaves.queue_path(scope)

    content, sha = common.get_json_file(args.repo, args.token, args.state_branch, path)
    if sha is None:
        print(f"nothing to clean up under {scope!r}", file=sys.stderr)
        return

    failed = content.get("failed", [])
    if failed:
        print(f"::warning::{len(failed)} region(s) ended this run permanently failed:", file=sys.stderr)
        for region_id in failed:
            print(f"::warning::  {region_id}", file=sys.stderr)

    try:
        common.update_json_file_with_retry(
            args.repo, args.token, args.state_branch, path,
            lambda _content: {},
            message=f"reset queue state for {scope!r}",
        )
        print(f"reset queue state under {scope!r}", file=sys.stderr)
    except RuntimeError as exc:
        print(f"::warning::failed to reset queue state under {scope!r}: {exc}", file=sys.stderr)
        print(f"::warning::a later cleanup run will retry", file=sys.stderr)


if __name__ == "__main__":
    main()
