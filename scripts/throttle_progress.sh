#!/usr/bin/env bash
# Throttles \r-redrawn output (a tool repainting one line in place, e.g.
# tile-join's "z/x/y" or tilemaker's per-tile/per-block counters) to at
# most one line per INTERVAL seconds, most recent wins; \n-terminated
# lines always print immediately. Splits purely on which byte terminated
# a chunk, not on message text, so it works for any \r-redrawing tool.
#
# Particularly worth using ahead of a GitHub Actions log: its viewer
# doesn't support \r as an in-place redraw, so a \r-heavy tool piped in
# unthrottled either floods the log with one line per redraw or renders
# as one unreadable blob, depending on the step.
#
# A pending \r line is dropped, not shown, once a real \n line arrives:
# the \n line already proves the tool moved on, so the stale redraw
# before it isn't worth another log line (flushing it anyway would leak
# one progress line per phase transition, on any tool that interleaves
# \n status lines with \r redraws throughout a run). It's still shown if
# nothing else supersedes it: once INTERVAL seconds pass idle (stalled
# source, via the read timeout below), once INTERVAL seconds pass since
# the last print, or at EOF.
#
# Usage: some_noisy_command 2>&1 | throttle_progress.sh <interval_seconds>
#
# No `set -e`: `read -t`'s non-zero exit on timeout/EOF is expected here,
# not an error to abort on.
set -u

interval="$1"

# partial_line: characters seen since the last \r or \n boundary, not yet
#   known whether a \r or \n will end it.
# pending_line / pending_is_set: the most recent \r-terminated redraw,
#   waiting for its turn to print (or to be dropped by a \n). A separate
#   flag is needed because an empty string is a valid pending value (two
#   \r's in a row with nothing between them).
# last_shown_at: epoch seconds of the last line this script printed.
partial_line=""
pending_line=""
pending_is_set=0

epoch_seconds() {
  # Bash builtin (no fork), unlike `date +%s`: matters here since this
  # runs once per \r seen, not once per printed line.
  printf -v NOW_EPOCH '%(%s)T' -1
}

epoch_seconds
last_shown_at="$NOW_EPOCH"

# Moves partial_line into pending_line, as a \r boundary or a stall
# timeout both do: either way, whatever was accumulating is now a
# complete (if possibly incomplete-looking) redraw candidate.
promote_partial_to_pending() {
  pending_line="$partial_line"
  pending_is_set=1
  partial_line=""
}

# Prints pending_line and resets the throttle clock, but only once
# `interval` seconds have passed since the last print.
show_pending_if_due() {
  (( pending_is_set )) || return
  epoch_seconds
  if (( NOW_EPOCH - last_shown_at >= interval )); then
    printf '%s\n' "$pending_line"
    last_shown_at="$NOW_EPOCH"
    pending_is_set=0
  fi
}

# \n: a real, complete line. Always prints immediately, and drops any
# stale pending redraw instead of showing it first: this line already
# proves the tool moved on.
handle_major_line() {
  pending_is_set=0
  printf '%s\n' "$partial_line"
  partial_line=""
}

# \r: the tool is repainting its progress line in place.
handle_redraw_boundary() {
  promote_partial_to_pending
  show_pending_if_due
}

# read timed out: the tool is still running but hasn't written anything
# for a full interval, not EOF. If it stalled mid-redraw (wrote some
# characters since the last boundary but hasn't reached the next \r/\n
# yet), that partial content is the most recent known state, so promote
# it too; otherwise the stall stays invisible and the eventually-arriving
# rest of the line glues onto it once output resumes.
handle_stall() {
  if [[ -n "$partial_line" ]]; then
    promote_partial_to_pending
  fi
  show_pending_if_due
}

# Both pending_line and partial_line can hold real, never-shown content
# once the stream ends: pending_line from a \r that never became due,
# partial_line from further output after it with no terminator yet.
# pending_line is chronologically first. Ends with `return 0`: this is
# the script's last function call, so whether there happened to be
# anything trailing here must not become the script's own exit status
# (GitHub Actions runs with pipefail, so a nonzero exit here would fail
# the whole step).
flush_remaining() {
  if (( pending_is_set )); then
    printf '%s\n' "$pending_line"
  fi
  if [[ -n "$partial_line" ]]; then
    printf '%s\n' "$partial_line"
  fi
  return 0
}

while true; do
  IFS= read -r -d '' -n 1 -t "$interval" char
  read_status=$?

  if (( read_status > 128 )); then
    handle_stall
    continue
  fi

  if (( read_status != 0 )); then
    break  # real EOF or read failure
  fi

  case "$char" in
    $'\n') handle_major_line ;;
    $'\r') handle_redraw_boundary ;;
    *) partial_line+="$char" ;;
  esac
done

flush_remaining
