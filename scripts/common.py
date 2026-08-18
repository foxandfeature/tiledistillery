"""Shared GitHub API helpers for the claim/timing scripts.

Everything here talks to the *calling* repository's own GitHub API (repo +
token are always passed in explicitly, read from GITHUB_REPOSITORY /
GITHUB_TOKEN by each script's CLI) — never a third-party service. See
docs/ARCHITECTURE.md "Locking" and "Timing history" for why refs are used
for claims (atomic create-if-absent) and the Contents API for
state/timings.json (low-frequency, idempotent to retry).
"""

import base64
import json
import random
import time
import urllib.parse

import requests

API_BASE = "https://api.github.com"
_HEADERS_ACCEPT = "application/vnd.github+json"
_API_VERSION = "2022-11-28"

# GitHub's secondary rate limits ask for backoff, not an immediate retry; a
# full worker fleet (worker_count workers, no topup round to spread the load
# across anymore — see _pipeline.yml) hammering the same repo's git/refs API
# at once can plausibly hit this, worst-case near the tail of a run when most
# regions are already done and most workers are simultaneously racing over
# the few still left. Doubling from a 4s base (this is the GitHub API being
# asked to recover, not this runner, same "give the other side room"
# reasoning as the curl download retry) up to _BACKOFF_CAP_S; "full jitter"
# (sleep a random amount in [0, backoff), not the backoff itself) keeps
# retrying workers from re-colliding in lockstep on the next attempt, which
# a fixed exponential delay would not.
_MAX_RETRIES = 8
_BACKOFF_BASE_S = 4
_BACKOFF_CAP_S = 60


def _backoff_sleep(attempt):
    time.sleep(random.uniform(0, min(_BACKOFF_CAP_S, _BACKOFF_BASE_S * (2 ** attempt))))


def _headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": _HEADERS_ACCEPT,
        "X-GitHub-Api-Version": _API_VERSION,
    }


class TokenPermissionError(RuntimeError):
    """A 403 that isn't GitHub's secondary-rate-limit signal — the token
    itself lacks the needed scope, so retrying only wastes _MAX_RETRIES
    rounds of backoff before failing anyway. Raised immediately instead."""


def _permission_denied_message(resp):
    """None if this 403 is (or might be) a retryable secondary rate limit;
    otherwise the GitHub error message, for a message that explains what to
    fix instead of a bare HTTPError. GitHub's secondary-rate-limit 403s
    always mention 'rate limit' in the message; permission-denied 403s
    ('Resource not accessible by integration', missing/insufficient scopes)
    never do, and no amount of retrying fixes them."""
    try:
        message = resp.json().get("message", "")
    except ValueError:
        return None
    if "rate limit" in message.lower():
        return None
    return message


def _request(method, path, token, **kwargs):
    url = f"{API_BASE}{path}"
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.request(method, url, headers=_headers(token), timeout=30, **kwargs)
        except requests.RequestException as exc:
            last_exc = exc
            _backoff_sleep(attempt)
            continue
        if resp.status_code == 403:
            message = _permission_denied_message(resp)
            if message is not None:
                raise TokenPermissionError(
                    f"GitHub rejected {method} {path} as 403 Forbidden: {message!r}. "
                    "This is a token-permission problem, not a rate limit, so retrying "
                    "won't help: the calling workflow must grant "
                    "`permissions: contents: write` (see docs/ARCHITECTURE.md "
                    "'State lives in the caller's repo, not this one')."
                )
        if resp.status_code in (403, 429) or resp.status_code >= 500:
            # GitHub's secondary-rate-limit responses often carry a
            # Retry-After (seconds) that's a more authoritative wait time
            # than a blind guess — honor it, still jittered a little so a
            # whole fleet released on the same limit doesn't retry in the
            # exact same instant, but otherwise fall back to backoff.
            retry_after = resp.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    time.sleep(float(retry_after) + random.uniform(0, 1))
                except ValueError:
                    _backoff_sleep(attempt)
            else:
                _backoff_sleep(attempt)
            continue
        return resp
    if last_exc is not None:
        raise last_exc
    return resp  # last response, whatever it was, after exhausting retries


import re

_REF_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def sanitize_ref_component(s):
    """Makes one path segment safe to use inside a git ref name. Region IDs
    (Geofabrik ids) and output_basenames are expected to already be
    plain slugs; this is defensive, not a primary validation layer."""
    cleaned = _REF_UNSAFE.sub("-", s).strip("-.")
    return cleaned or "-"


def get_ref_sha(repo, token, ref):
    """ref: without the leading 'refs/', e.g. 'heads/state'. None if absent."""
    resp = _request("GET", f"/repos/{repo}/git/ref/{ref}", token)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["object"]["sha"]


def create_ref(repo, token, ref, sha):
    """ref: without the leading 'refs/'. Returns True if created, False if it
    already existed (422) — the atomic compare-and-swap this whole locking
    scheme relies on. Raises on any other error."""
    resp = _request(
        "POST",
        f"/repos/{repo}/git/refs",
        token,
        json={"ref": f"refs/{ref}", "sha": sha},
    )
    if resp.status_code == 201:
        return True
    if resp.status_code == 422:
        return False
    resp.raise_for_status()
    return False


def delete_ref(repo, token, ref):
    """ref: without the leading 'refs/'. No-op if already gone."""
    resp = _request("DELETE", f"/repos/{repo}/git/refs/{ref}", token)
    if resp.status_code not in (204, 404, 422):
        resp.raise_for_status()


def list_matching_refs(repo, token, prefix):
    """prefix: without the leading 'refs/', e.g. 'claims'. Returns the full
    'refs/...' name of every ref starting with 'refs/<prefix>/'."""
    quoted = urllib.parse.quote(prefix, safe="/")
    resp = _request("GET", f"/repos/{repo}/git/matching-refs/{quoted}", token)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return [entry["ref"] for entry in resp.json()]


# The empty tree's SHA is a git constant (content-addressed hash of zero
# entries), identical in every repository, so it needs no API call to obtain.
_EMPTY_TREE_SHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def ensure_branch(repo, token, branch):
    """Creates an empty orphan `branch` if it doesn't exist yet, so a caller
    never has to set up the state branch by hand (see docs/ARCHITECTURE.md
    "State lives in the caller's repo, not this one"). Returns the branch
    tip sha either way."""
    sha = get_ref_sha(repo, token, f"heads/{branch}")
    if sha is not None:
        return sha
    resp = _request(
        "POST",
        f"/repos/{repo}/git/commits",
        token,
        json={"message": f"init {branch} branch", "tree": _EMPTY_TREE_SHA, "parents": []},
    )
    resp.raise_for_status()
    commit_sha = resp.json()["sha"]
    if create_ref(repo, token, f"heads/{branch}", commit_sha):
        return commit_sha
    # Another worker created it concurrently between our GET and our POST.
    return get_ref_sha(repo, token, f"heads/{branch}")


def get_json_file(repo, token, branch, path):
    """Returns (content_dict, sha). sha is None (content is {}) if the file
    doesn't exist yet on that branch."""
    quoted = urllib.parse.quote(path)
    resp = _request(
        "GET",
        f"/repos/{repo}/contents/{quoted}",
        token,
        params={"ref": branch},
    )
    if resp.status_code == 404:
        return {}, None
    resp.raise_for_status()
    body = resp.json()
    raw = base64.b64decode(body["content"])
    return json.loads(raw), body["sha"]


def put_json_file(repo, token, branch, path, content_dict, sha, message):
    """Returns True on success. Returns False on a 409/422 conflict (branch
    moved since `sha` was read) so the caller can re-fetch, re-merge the
    mutation, and retry — see update_json_file_with_retry. Also False on a
    403 that reaches this point: _request already raises TokenPermissionError
    immediately for a genuine permission-denied 403, so a 403 surviving to
    here can only be the ambiguous/rate-limit-shaped kind that exhausted
    _request's own retries — worth another read-modify-write cycle (with
    update_json_file_with_retry's own backoff) rather than a hard crash."""
    quoted = urllib.parse.quote(path)
    payload = {
        "message": message,
        "content": base64.b64encode(
            json.dumps(content_dict, indent=2, sort_keys=True).encode() + b"\n"
        ).decode(),
        "branch": branch,
    }
    if sha is not None:
        payload["sha"] = sha
    resp = _request("PUT", f"/repos/{repo}/contents/{quoted}", token, json=payload)
    if resp.status_code in (200, 201):
        return True
    if resp.status_code in (403, 409, 422):
        return False
    resp.raise_for_status()
    return False


def update_json_file_with_retry(repo, token, branch, path, mutate_fn, message, max_attempts=8):
    """mutate_fn(content_dict) -> new_content_dict. Retries the whole
    read-mutate-write cycle on a conflicting concurrent write from another
    worker (see docs/ARCHITECTURE.md "Locking": this file is shared and
    low-frequency enough that retry-on-conflict, not an atomic primitive, is
    the right tool)."""
    for attempt in range(max_attempts):
        content, sha = get_json_file(repo, token, branch, path)
        new_content = mutate_fn(content)
        if put_json_file(repo, token, branch, path, new_content, sha, message):
            return new_content
        time.sleep(_BACKOFF_BASE_S * (2 ** attempt) * (0.5 + 0.5 * (attempt % 2)))
    raise RuntimeError(f"update_json_file_with_retry: gave up on {path} after {max_attempts} attempts")
