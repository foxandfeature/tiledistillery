#!/usr/bin/env python3
"""Deletes every ref under this run's claim scope on the calling
repository's own `state` branch, run with `if: always()` at the end of
`_pipeline.yml` so the next run of the same build starts clean (see
docs/ARCHITECTURE.md "Locking" — claims are deliberately not run-ID-scoped,
so this is what actually resets state between runs). Best effort: a ref
that fails to delete (for example a persistent GitHub API error) is logged
and skipped instead of aborting the rest of the cleanup, since a later
run's cleanup will pick it up again from a fresh `list_matching_refs`."""

import argparse
import sys

import claim
import common


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--output-basename", required=True)
    args = parser.parse_args()

    scope = claim.scope_prefix(args.output_basename)
    refs = common.list_matching_refs(args.repo, args.token, f"claims/{scope}")

    failed = [r for r in refs if r.endswith("/failed")]
    if failed:
        print(f"::warning::{len(failed)} region(s) ended this run permanently failed:", file=sys.stderr)
        for r in failed:
            print(f"::warning::  {r}", file=sys.stderr)

    skipped = []
    for ref in refs:
        try:
            common.delete_ref(args.repo, args.token, ref[len("refs/"):])
        except Exception as exc:
            skipped.append(ref)
            print(f"::warning::failed to delete {ref}: {exc}", file=sys.stderr)

    print(f"cleaned up {len(refs) - len(skipped)}/{len(refs)} claim ref(s) under {scope!r}", file=sys.stderr)
    if skipped:
        print(f"::warning::{len(skipped)} claim ref(s) left behind under {scope!r}; a later cleanup run will retry them", file=sys.stderr)


if __name__ == "__main__":
    main()
