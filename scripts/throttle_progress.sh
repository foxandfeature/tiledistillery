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
# \n lines identical to the one just shown collapse too (e.g. a per-group
# stats line a tool logs once per group, with no \r involved at all):
# instead of reprinting each occurrence, the first is shown immediately
# as always, without its trailing newline yet (last_shown_open), and
# later repeats are held back and folded into that same still-open line
# as a "(Nx)" suffix rather than a full reprint, applying the same "at
# most once per INTERVAL, or when something ends the run" throttling as
# \r redraws get. Unlike a superseded \r redraw, a finished repeat run
# isn't discarded, since the count itself is the useful part.
#
# Usage: some_noisy_command 2>&1 | throttle_progress.sh <interval_seconds> [exclude_ere]
#
# exclude_ere (optional) is an extended regex matched against each
# complete \n-terminated line; matches are dropped instead of printed.
# For a caller-known banner that's pure noise (e.g. a wrapper script's
# own hardcoded warning, unconditional and unrelated to this particular
# run), not a general log filter: it only ever sees whole \n lines, never
# a still-accumulating \r redraw, so it can't cut a redraw off mid-line.
#
# No `set -e`: `read -t`'s non-zero exit on timeout/EOF is expected here,
# not an error to abort on.
set -u

interval="$1"
exclude_ere="${2:-}"

# partial_line: characters seen since the last \r or \n boundary, not yet
#   known whether a \r or \n will end it.
# pending_line / pending_is_set: the most recent \r-terminated redraw,
#   waiting for its turn to print (or to be dropped by a \n). A separate
#   flag is needed because an empty string is a valid pending value (two
#   \r's in a row with nothing between them).
# last_shown_line / last_shown_is_set: the most recent \n line actually
#   printed, kept around purely to recognize the next \n line as a repeat
#   of it (same empty-string-is-valid reasoning as pending_is_set).
# last_shown_open: last_shown_line was printed without its trailing
#   newline yet, in case it turns out to be the start of a repeat run
#   whose count needs to land on that same line. Closed (newline emitted,
#   with or without a "(Nx)" suffix) as soon as anything else needs to
#   print: a different \n line, a \r redraw, or EOF.
# repeat_count: how many times last_shown_line has recurred since it was
#   printed, not yet folded into a shown "(Nx)" update.
# last_shown_at: epoch seconds of the last line this script printed,
#   shared by \r-pending and \n-repeat throttling: one clock for "how
#   stale is the log right now".
partial_line=""
pending_line=""
pending_is_set=0
last_shown_line=""
last_shown_is_set=0
last_shown_open=0
repeat_count=0

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
# `interval` seconds have passed since the last print. Closes out a
# still-open last_shown_line first: a \r redraw is a new, unrelated line,
# not a continuation of whatever repeat run was pending on that one.
show_pending_if_due() {
  (( pending_is_set )) || return
  epoch_seconds
  if (( NOW_EPOCH - last_shown_at >= interval )); then
    flush_repeat
    printf '%s\n' "$pending_line"
    last_shown_at="$NOW_EPOCH"
    pending_is_set=0
  fi
}

# True if $1 matches exclude_ere (always false when no pattern was given,
# so callers that don't pass one keep every \n line, unchanged).
line_excluded() {
  [[ -n "$exclude_ere" && "$1" =~ $exclude_ere ]]
}

# Appends a "(Nx)" update onto the still-open last_shown_line, but only
# once `interval` seconds have passed since the last print (mirrors
# show_pending_if_due, and shares its clock). No newline: the run may
# still keep going, in which case a later due update overwrites nothing
# but simply appends again, and only flush_repeat closes the line for
# good. No re-printing of last_shown_line itself, unlike a superseded \r
# redraw: it's still sitting there on the terminal/log from when it was
# first shown.
show_repeat_if_due() {
  (( repeat_count > 0 )) || return
  epoch_seconds
  if (( NOW_EPOCH - last_shown_at >= interval )); then
    printf ' (%dx)' "$(( repeat_count + 1 ))"
    last_shown_at="$NOW_EPOCH"
    repeat_count=0
  fi
}

# Closes out last_shown_line for good once its repeat run is known to be
# over (a different line arrived, a \r redraw needs the terminal, or
# EOF). If repeats accrued since the last show_repeat_if_due, appends a
# final "(Nx)" first; either way, emits the newline that was withheld
# when the line was first printed. A no-op once last_shown_open is
# already 0, so callers can call it unconditionally.
flush_repeat() {
  (( last_shown_open )) || return
  if (( repeat_count > 0 )); then
    printf ' (%dx)' "$(( repeat_count + 1 ))"
    repeat_count=0
  fi
  printf '\n'
  last_shown_open=0
}

# \n: a real, complete line. Drops any stale pending redraw first: this
# line already proves the tool moved on. An excluded line is dropped
# outright, before touching repeat-tracking state at all, so it can't
# itself break up a run of repeats on either side of it. Otherwise: a
# repeat of last_shown_line is held back (see show_repeat_if_due); a
# genuinely new line flushes whatever repeat count was pending, then
# prints immediately, same as ever.
handle_major_line() {
  pending_is_set=0
  local line="$partial_line"
  partial_line=""

  line_excluded "$line" && return

  if (( last_shown_is_set )) && [[ "$line" == "$last_shown_line" ]]; then
    (( repeat_count++ ))
    show_repeat_if_due
    return
  fi

  flush_repeat
  printf '%s' "$line"
  last_shown_line="$line"
  last_shown_is_set=1
  last_shown_open=1
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
  show_repeat_if_due
}

# pending_line, partial_line, and a pending repeat count can all hold
# real, never-shown content once the stream ends: pending_line from a \r
# that never became due, repeat_count from a repeat run that never became
# due, partial_line from further output after either with no terminator
# yet. flush_repeat first since it reports on last_shown_line, the
# chronologically earliest of the three. Ends with `return 0`: this is
# the script's last function call, so whether there happened to be
# anything trailing here must not become the script's own exit status
# (GitHub Actions runs with pipefail, so a nonzero exit here would fail
# the whole step).
flush_remaining() {
  flush_repeat
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
