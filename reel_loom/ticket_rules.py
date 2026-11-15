"""Pure render-ticket lifecycle rules.

Every decision in this module takes plain values and returns a value or raises a
domain error. There is no database, file system or encoder access here, so the
rules can be exercised on their own without SQLite. The persistence adapter
(`ticket_store`) applies decisions handed to it; the application service
(`tickets`) asks these rules and owns the transaction that commits the result.
"""

from .errors import RenderCancelled, require
from .values import integer, object_body

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
CANCELLED = "cancelled"

STATES = (QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED)
TERMINAL = frozenset((SUCCEEDED, FAILED, CANCELLED))
RETRYABLE = frozenset((FAILED, CANCELLED))

# A queued ticket can be cancelled outright; a running one only records a
# request because the encoder still owns the scratch output.
CANCEL_NOW = "cancel_now"
CANCEL_REQUEST = "cancel_request"
NO_EFFECT = None

# Claim order is part of the queue policy: oldest creation time, then id.
QUEUE_ORDER = ("created", "id")

MATRIX_LIMIT = 12


def enqueue_body(body):
    """Validate the shape of a single-render request without touching storage."""
    return object_body(body, ("reel", "revision", "look", "look_version"), ("reel", "look"))


def matrix_body(body):
    """Validate the outer render-matrix request and return its look entries."""
    object_body(body, ("reel", "revision", "looks"), ("reel", "looks"))
    looks = body["looks"]
    require(isinstance(looks, list) and 1 <= len(looks) <= MATRIX_LIMIT,
            "batch_size", f"A render matrix requires 1..{MATRIX_LIMIT} looks")
    return looks


def matrix_entry(item):
    """Validate one look reference inside a matrix, without resolving it."""
    object_body(item, ("id", "version"), ("id",))
    if "version" in item:
        integer(item["version"], "version", 1)
    return item


def register_look(seen, identity):
    """Require that a matrix freezes each look version at most once."""
    require(identity not in seen, "duplicate_look", "A look version cannot repeat within a matrix")
    seen.add(identity)


def claimable(state):
    require(state == QUEUED, "ticket_claim", "Only a queued render can start", 409)
    return RUNNING


def cancel_effect(state):
    if state == QUEUED:
        return CANCEL_NOW
    if state == RUNNING:
        return CANCEL_REQUEST
    return NO_EFFECT


def failure_state(state, cancel_requested, error_code=None):
    if state != RUNNING:
        return NO_EFFECT
    return CANCELLED if cancel_requested or error_code == "render_cancelled" else FAILED


def retry_attempt(state, attempt):
    require(state in RETRYABLE, "retry_state", "Only failed or cancelled renders can retry", 409)
    return attempt + 1


def recovery_state(state, cancel_requested):
    if state != RUNNING:
        return NO_EFFECT
    return CANCELLED if cancel_requested else FAILED


def require_publishable(state, cancel_requested):
    """Decide whether an encoded result may become the ticket's rendition.

    Cancellation wins at the same commit boundary: a requested cancellation is
    rejected before any artifact is registered.
    """
    if cancel_requested:
        raise RenderCancelled()
    require(state == RUNNING, "ticket_state", "Only an active render can commit", 409)


def require_packagable(state):
    require(state == SUCCEEDED, "imprint_state", "Only completed renditions can be packaged", 409)


def capped_progress(value):
    return min(value, .99)


def batch_state(states):
    """Aggregate independently executed matrix members into the matrix state."""
    require(bool(states), "empty_batch", "A render matrix requires member tickets", 409)
    if all(state == SUCCEEDED for state in states):
        return SUCCEEDED
    if all(state in TERMINAL for state in states):
        return "finished_with_failures"
    if any(state in TERMINAL or state == RUNNING for state in states):
        return RUNNING
    return QUEUED
