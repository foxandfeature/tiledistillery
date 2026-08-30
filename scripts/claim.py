#!/usr/bin/env python3
"""Claim lifecycle over one shared JSON queue file, against the calling
repository's own `state` branch (`state/queue/<scope>.json`, seeded by
leaves.py's write_queue). See docs/ARCHITECTURE.md "Locking" for the file's
shape, why claiming happens in small batches (`--batch-size`), and why
`done`/`failed` buffer outcomes locally instead of touching the shared
file directly.

Subcommands:

    claim.py run-worker    --config ... --process ... --tilemaker-image ...
    claim.py flush-timings --timings-dir DIR ...

`run-worker` is one worker's whole claim/download/build/report cycle,
looping until the queue is empty; see cmd_run_worker's docstring. A region
is marked permanently `failed` (a single strike, not a retry counter) the
first time a build fails: retrying transient failures (a flaky download, a
flaky GitHub API call) happens before that point, on the same runner (see
cmd_run_worker and common.py's `_request`), since retrying a genuinely
broken region (bad Geofabrik data, a real 404, a build failing on
tilemaker's own terms) from here would just waste other workers' time on
the same failure. `flush-timings` prints a one-line human-readable summary
to stdout; see cmd_flush_timings's docstring.
"""

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

import requests

import common
import leaves


def scope_prefix(output_basename):
    return common.sanitize_ref_component(output_basename)


class ClaimState:
    """Parsed view of one scope's queue file (`{"remaining": [{"id":...,
    "pbf_url":...}, ...], "lock": [...], "done": [...], "failed": [...]}`,
    the last three each a list of region ids). `lock` is the mutex; see
    docs/ARCHITECTURE.md "Locking" for why it has no lease/timestamp or
    staleness sweep, and what a crashed worker costs as a result.
    """

    def __init__(self, content):
        self.remaining = content.get("remaining", [])
        self.lock = set(content.get("lock", []))
        self.done = set(content.get("done", []))
        self.failed = set(content.get("failed", []))

    def unavailable(self, region_id):
        return region_id in self.done or region_id in self.failed or region_id in self.lock


def load_state(repo, token, state_branch, scope):
    content, _ = common.get_json_file(repo, token, state_branch, leaves.queue_path(scope))
    return ClaimState(content)


# Small on purpose: bounds the crash blast radius (see ClaimState's
# docstring). See docs/ARCHITECTURE.md "Locking" for the full tradeoff.
_DEFAULT_BATCH_SIZE = 5


def _claim_batch(repo, token, state_branch, scope, batch_size):
    """One read-modify-write claims up to `batch_size` regions at once. See
    docs/ARCHITECTURE.md "Locking" for why batching, and why retry-on-conflict
    here can't produce an overlapping batch."""
    claimed = []

    def mutate(content):
        claimed.clear()
        state = ClaimState(content)
        remaining = list(state.remaining)
        take = remaining[:batch_size]
        del remaining[:batch_size]
        claimed.extend(take)
        content["remaining"] = remaining
        content["lock"] = sorted(state.lock | {r["id"] for r in take})
        return content

    common.update_json_file_with_retry(
        repo, token, state_branch, leaves.queue_path(scope), mutate,
        message=f"claim up to {batch_size} region(s) for {scope!r}",
    )
    return claimed


def _next_region(args, batch):
    """One region popped from `batch` (an in-memory list, refilled in place
    from the shared queue via _claim_batch once empty), or None once the
    shared queue is exhausted. Used by cmd_run_worker, whose loop owns
    `batch` for its own lifetime, so a fresh claim is only ever one shared-
    file read-modify-write per `--batch-size` regions, not one per call."""
    if not batch:
        common.ensure_branch(args.repo, args.token, args.state_branch)
        scope = scope_prefix(args.output_basename)
        try:
            batch.extend(_claim_batch(args.repo, args.token, args.state_branch, scope, args.batch_size))
        except common.TokenPermissionError:
            raise
        except (requests.RequestException, RuntimeError) as exc:
            # Reached only after the retries inside _request/
            # update_json_file_with_retry are already exhausted: a
            # persistent outage shouldn't crash this worker, so treat it
            # like an empty queue instead, same as cmd_flush_timings does.
            print(f"::warning::claim batch for {args.output_basename} failed: {exc}", file=sys.stderr)

    if not batch:
        return None

    return batch.pop(0)


def _record_done(args, region_id, duration_seconds):
    # No API call: buffered locally, merged later by cmd_flush_timings. See
    # docs/ARCHITECTURE.md "Locking" for why not straight to the shared file.
    with open(args.timings_buffer, "a") as f:
        f.write(json.dumps({
            "region_id": region_id,
            "status": "done",
            "duration_seconds": duration_seconds,
        }) + "\n")


def cmd_flush_timings(args):
    """Merges every worker's buffered outcomes into the shared timing-history
    file and the shared queue's `done`/`failed` lists, called once by
    `record-timings` (_pipeline.yml). See docs/ARCHITECTURE.md "Timing
    history & queue ordering" for the single-writer/wrapped-failures
    rationale.

    `timings_dir` is searched recursively (each worker's artifact lands in
    its own subdirectory); missing or empty means nothing was buffered, a
    no-op."""
    entries = []
    if args.timings_dir and pathlib.Path(args.timings_dir).is_dir():
        for path in sorted(pathlib.Path(args.timings_dir).rglob("*")):
            if not path.is_file():
                continue
            with open(path) as f:
                entries.extend(json.loads(line) for line in f if line.strip())

    if not entries:
        print("nothing buffered, skipping flush")
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
            lock.discard(entry["region_id"])
            done.add(entry["region_id"])
        for entry in failed_entries:
            lock.discard(entry["region_id"])
            failed.add(entry["region_id"])
        content["remaining"] = state.remaining
        content["lock"] = sorted(lock)
        content["done"] = sorted(done)
        content["failed"] = sorted(failed)
        return content

    try:
        common.update_json_file_with_retry(
            args.repo, args.token, args.state_branch, leaves.queue_path(scope), apply_claims,
            message=f"record {len(done_entries)} done, {len(failed_entries)} failed region(s) for {scope!r}",
        )
    except RuntimeError as exc:
        print(f"::warning::claims flush failed for {len(entries)} region(s): {exc}", file=sys.stderr)

    print(f"flushed {len(entries)} region outcome(s): {len(done_entries)} done, {len(failed_entries)} failed")


def _record_failed(args, region_id, error):
    # See _record_done: same "no API call, buffer locally" reasoning
    # applies here.
    with open(args.timings_buffer, "a") as f:
        f.write(json.dumps({
            "region_id": region_id,
            "status": "failed",
            "error": error,
        }) + "\n")
    if error:
        print(f"::warning::{region_id} failed: {error}", file=sys.stderr)


# Fed through throttle_progress.sh (see that script for the \r-vs-\n
# reasoning): both curl's own progress meter and tilemaker's several
# per-tile/per-block counters redraw in place via \r, while genuine
# phase-transition messages use a real newline.
_THROTTLE_SCRIPT = str(pathlib.Path(__file__).parent / "throttle_progress.sh")
# Regions run "several-hundred-MB" downloads (see docs/ARCHITECTURE.md
# "Distribution"); shorter than the build interval below since a stalled
# or slow transfer is worth surfacing sooner than a build phase is.
_DOWNLOAD_PROGRESS_INTERVAL = "15"
_TILEMAKER_PROGRESS_INTERVAL = "60"


def _download_pbf(url, dest):
    """Exit status of downloading `url` to `dest` via curl, piping curl's
    own stderr progress meter (% complete, size, transfer speed, estimated
    time left) through throttle_progress.sh so the GitHub Actions log gets
    periodic single-line updates instead of either silence (the prior
    `-s`) or an unreadable flood of \r redraws. `--retry` (no
    `--retry-all-errors`) retries only transient failures (timeouts, 5xx,
    408/429), not a 404/other 4xx: Geofabrik saying the region is genuinely
    gone shouldn't cost 5 attempts before falling through to the caller's
    `_record_failed`. No `--retry-delay`: curl's own default backoff is
    already what we want here. Returns curl's exit code, not the throttle
    pipeline's, which only ever reformats output (mirrors
    _run_docker_build)."""
    curl_proc = subprocess.Popen(
        ["curl", "-fL", "--retry", "5", "--retry-connrefused", url, "-o", str(dest)],
        stderr=subprocess.PIPE,
    )
    throttle_proc = subprocess.Popen([_THROTTLE_SCRIPT, _DOWNLOAD_PROGRESS_INTERVAL], stdin=curl_proc.stderr)
    curl_proc.stderr.close()  # let curl_proc get SIGPIPE if throttle_proc exits early
    throttle_proc.wait()
    return curl_proc.wait()


# tilemaker's docker-entrypoint.sh (github.com/systemed/tilemaker
# resources/docker-entrypoint.sh) echoes this exact 5-line banner
# unconditionally, before tilemaker itself even starts, regardless of
# whether --store is already passed (which it is, below): not a signal
# about *this* run, just fixed noise on every single region. No flag
# suppresses it (it's not tilemaker printing it), so it's filtered here
# instead of patching the upstream image. Matched line-for-line rather
# than a loose "DOCKER WARNING:"/"-+" prefix so a real, different Docker
# or tilemaker warning (a distinct message, or in some future image
# version) isn't silently swallowed too.
_TILEMAKER_NOISE_ERE = (
    r'^-{80}$'
    r'|^DOCKER WARNING: Docker Out Of Memory handling can be unreliable\.$'
    r'|^DOCKER WARNING: If your program unexpectedly exits, it might have been terminated by the Out Of Memory killer without a visible notice\.$'
    r'|^DOCKER WARNING: The --store option can be used to partly reduce memory usage\.$'
)


def _run_docker_build(cmd):
    """Runs a tilemaker `docker run` invocation, piping its combined
    stdout/stderr through throttle_progress.sh (dropping _TILEMAKER_NOISE_ERE
    lines along the way). Returns docker's exit code (not the throttle
    pipeline's, which only ever reformats output)."""
    sys.stdout.flush()  # keep prior prints ordered before the pipeline's direct writes to the same fd
    docker_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    throttle_proc = subprocess.Popen(
        [_THROTTLE_SCRIPT, _TILEMAKER_PROGRESS_INTERVAL, _TILEMAKER_NOISE_ERE], stdin=docker_proc.stdout
    )
    docker_proc.stdout.close()  # let docker_proc get SIGPIPE if throttle_proc exits early
    throttle_proc.wait()
    return docker_proc.wait()


def cmd_run_worker(args):
    """One worker's claim/download/build/report loop, until the shared
    queue is exhausted (`claim-and-build`'s composite action step). See
    docs/ARCHITECTURE.md "Distribution" for why a worker loops over many
    regions, and "Multiple layers, one download" for why one claimed region
    builds every `--config`/`--process` pair against a single download and
    is reported failed as one atomic unit if any of them fails.
    """
    configs = args.config.split(",")
    processes = args.process.split(",")
    basenames = args.output_basename.split(",")

    shards_dir = pathlib.Path(args.shards_dir)
    shards_dir.mkdir(parents=True, exist_ok=True)
    pbf_path = pathlib.Path("region.osm.pbf")
    batch = []

    while True:
        region = _next_region(args, batch)
        if region is None:
            print("queue empty, worker exiting")
            return

        region_id = region["id"]
        safe_name = region_id.replace("/", "_")

        print(f"::group::{region_id}")
        print(f"downloading {region['pbf_url']}")
        start = time.monotonic()
        status = _download_pbf(region["pbf_url"], pbf_path)

        if status == 0:
            for config, process, basename in zip(configs, processes, basenames):
                # Store dir must live under the bind-mounted workspace, not
                # system /tmp (tempfile's default), since the container can
                # only see paths under /data (see the docker run below).
                store_dir = tempfile.mkdtemp(dir=".")
                try:
                    status = _run_docker_build([
                        "docker", "run", "--rm",
                        "--user", f"{os.getuid()}:{os.getgid()}",
                        "-v", f"{os.environ['GITHUB_WORKSPACE']}:/data", "-w", "/data",
                        args.tilemaker_image,
                        "--input", str(pbf_path),
                        "--output", str(shards_dir / f"{safe_name}--{basename}.mbtiles"),
                        "--config", config, "--process", process,
                        "--store", store_dir,
                    ])
                finally:
                    shutil.rmtree(store_dir, ignore_errors=True)
                # Not retried, unlike the curl download above: see the
                # module docstring's "single strike" note.
                if status != 0:
                    break

        pbf_path.unlink(missing_ok=True)
        duration_seconds = int(time.monotonic() - start)
        print("::endgroup::")

        if status == 0:
            _record_done(args, region_id, duration_seconds)
        else:
            for shard in shards_dir.glob(f"{safe_name}--*.mbtiles"):
                shard.unlink()
            _record_failed(args, region_id, f"build failed (exit {status})")


def _add_common_args(p):
    p.add_argument("--repo", required=True, help="owner/repo of the calling repository")
    p.add_argument("--token", required=True)
    p.add_argument("--state-branch", default="state")
    p.add_argument("--output-basename", required=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_flush = sub.add_parser("flush-timings")
    _add_common_args(p_flush)
    p_flush.add_argument("--timings-dir", required=True, help="Directory to search recursively for workers' uploaded timing-buffer files.")
    p_flush.set_defaults(func=cmd_flush_timings)

    p_run = sub.add_parser("run-worker")
    _add_common_args(p_run)
    p_run.add_argument("--batch-size", type=int, default=_DEFAULT_BATCH_SIZE, help="See _DEFAULT_BATCH_SIZE.")
    p_run.add_argument("--timings-buffer", required=True, help="Local file this worker's outcomes are appended to; see cmd_flush_timings.")
    p_run.add_argument("--config", required=True, help="Comma-separated tilemaker JSON layer config path(s), matched 1:1 with --process/--output-basename.")
    p_run.add_argument("--process", required=True, help="Comma-separated tilemaker Lua process script path(s), matched 1:1 with --config/--output-basename.")
    p_run.add_argument("--shards-dir", default="shards", help="Where each region's <region-id>--<output_basename>.mbtiles shards are written.")
    p_run.add_argument("--tilemaker-image", required=True, help="e.g. ghcr.io/systemed/tilemaker:master; see docs/ARCHITECTURE.md 'Tile-build engine: tilemaker'.")
    p_run.set_defaults(func=cmd_run_worker)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
