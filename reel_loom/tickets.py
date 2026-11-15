"""Render-ticket application service.

This service is the single owner of each ticket business operation's SQLite
transaction: enqueue, claim, cancel, failure, retry, startup recovery, rendition
publication and the matrix's all-or-nothing enqueue. It asks `ticket_rules` for
every decision and hands the outcome to `TicketStore`, which only records it.
Reel/look/source freezing is a planning capability invoked here while the
enqueue transaction is open; the persistence layer never resolves or edits
reels, looks or source media.

Publication is the whole commit for "encoded bytes become a rendition": the blob
registration, the artifact row and the succeeded state are written in one
transaction, so a cancellation observed in that transaction rolls all of them
back. The renderer and the HTTP layer never re-open that transaction themselves.
"""

from . import ticket_rules
from .errors import LoomError
from .ticket_store import TicketStore
from .values import identifier, now


class Tickets:
    def __init__(self, db, planner, keys, blobs):
        self.db, self.planner, self.keys, self.blobs = db, planner, keys, blobs
        self.store = TicketStore(db)

    # -- reads --------------------------------------------------------------

    def get(self, ticket_id, db=None):
        if db is None:
            with self.db.read() as read:
                return self.store.load(read, ticket_id)
        return self.store.load(db, ticket_id)

    def list(self, state=None):
        with self.db.read() as db:
            return [self.store.load(db, ticket_id) for ticket_id in self.store.ids(db, state)]

    def cancellation_requested(self, ticket_id):
        with self.db.read() as db:
            return self.store.cancellation_flag(db, ticket_id) is not False

    def publishable(self, db, ticket_id):
        """Read the publication decision inside the caller's transaction."""
        stored = self.store.publish_state(db, ticket_id)
        if stored is None:
            raise LoomError("ticket_missing", "Render ticket disappeared", 409)
        ticket_rules.require_publishable(*stored)

    # -- enqueue ------------------------------------------------------------

    def enqueue(self, body, key=None):
        ticket_rules.enqueue_body(body)
        with self.db.write_unit() as db:
            previous = self.keys.replay(db, "tickets", key, body)
            if previous:
                return previous
            snapshot = self.planner.freeze(db, body["reel"], body.get("revision"),
                                           body["look"], body.get("look_version"))
            ticket_id, created = identifier("render"), now()
            self.stage(db, snapshot, ticket_id, created)
            result = self.get(ticket_id, db)
            return self.keys.record(db, "tickets", key, body, result)

    def stage(self, db, snapshot, ticket_id, created, event_kind="queued", payload=None):
        """Add one frozen queued ticket inside a caller-owned transaction."""
        self.store.insert_queued(db, ticket_id, snapshot, created, event_kind,
                                 {"fingerprint": snapshot["fingerprint"]} if payload is None else payload)
        return ticket_id

    def submit_matrix(self, body, key=None):
        """Freeze every look and enqueue all members inside one transaction.

        Validation happens before the write unit opens; any freeze or insertion
        failure inside the unit rolls every member and its event back, so a
        matrix is all queued or entirely absent.
        """
        looks = ticket_rules.matrix_body(body)
        with self.db.write_unit() as db:
            previous = self.keys.replay(db, "render_matrix", key, body)
            if previous:
                return previous
            snapshots, identities = [], set()
            for item in looks:
                ticket_rules.matrix_entry(item)
                snapshot = self.planner.freeze(db, body["reel"], body.get("revision"),
                                               item["id"], item.get("version"))
                identity = (snapshot["look"]["id"], snapshot["look"]["version"])
                ticket_rules.register_look(identities, identity)
                snapshots.append(snapshot)
            batch_id, created, ids = identifier("matrix"), now(), []
            for snapshot in snapshots:
                ticket_id = identifier("render")
                self.stage(db, snapshot, ticket_id, created, "matrix_queued",
                           {"matrix": batch_id, "fingerprint": snapshot["fingerprint"]})
                ids.append(ticket_id)
            self.db.event(db, batch_id, "matrix_submitted", {"tickets": ids})
            result = {"id": batch_id, "tickets": [self.get(x, db) for x in ids]}
            return self.keys.record(db, "render_matrix", key, body, result)

    # -- queue mutations ----------------------------------------------------

    def claim(self):
        with self.db.write_unit() as db:
            ticket_id = self.store.next_queued(db, ticket_rules.QUEUE_ORDER)
            if ticket_id is None:
                return None
            # BEGIN IMMEDIATE excludes other writers, so the row read here is
            # the row the update acts on; mark_started also keeps the
            # queued->running predicate as a second line of defence.
            ticket_rules.claimable(self.store.row(db, ticket_id)["state"])
            if not self.store.mark_started(db, ticket_id, now()):
                return None
            self.db.event(db, ticket_id, "started", {})
            return self.store.load(db, ticket_id)

    def progress(self, ticket_id, value):
        with self.db.write_unit() as db:
            self.store.set_progress(db, ticket_id, ticket_rules.capped_progress(value), now())

    def cancel(self, ticket_id):
        with self.db.write_unit() as db:
            ticket = self.get(ticket_id, db)
            effect = ticket_rules.cancel_effect(ticket["state"])
            if effect is ticket_rules.NO_EFFECT:
                return ticket
            moment = now()
            if effect == ticket_rules.CANCEL_NOW:
                self.store.mark_cancelled(db, ticket_id, moment)
            else:
                self.store.request_cancel(db, ticket_id, moment)
            self.db.event(db, ticket_id, "cancel_requested", {})
            return self.get(ticket_id, db)

    def fail(self, ticket_id, error):
        with self.db.write_unit() as db:
            ticket = self.get(ticket_id, db)
            state = ticket_rules.failure_state(ticket["state"], ticket["cancel"], error.code)
            if state is ticket_rules.NO_EFFECT:
                return ticket
            self.store.mark_failed(db, ticket_id, state, error.as_dict(), now())
            self.db.event(db, ticket_id, state, error.as_dict())
            return self.get(ticket_id, db)

    def retry(self, ticket_id):
        with self.db.write_unit() as db:
            ticket = self.get(ticket_id, db)
            attempt = ticket_rules.retry_attempt(ticket["state"], ticket["attempt"])
            if not self.store.requeue(db, ticket_id, attempt, now()):
                raise LoomError("retry_state", "Only failed or cancelled renders can retry", 409)
            self.db.event(db, ticket_id, "retried", {"attempt": attempt})
            return self.get(ticket_id, db)

    def recover(self):
        with self.db.write_unit() as db:
            interrupted = 0
            error = {"error": "worker_interrupted", "detail": "Previous worker stopped before committing output",
                     "context": {}}
            for row in self.store.running(db):
                state = ticket_rules.recovery_state(row["state"], bool(row["cancel"]))
                if state is ticket_rules.NO_EFFECT:
                    continue
                self.store.mark_recovered(db, row["id"], state, error, now())
                self.db.event(db, row["id"], "recovered", {"state": state})
                interrupted += 1
            return {"interrupted": interrupted}

    # -- publication --------------------------------------------------------

    def publish(self, ticket_id, path, metadata):
        """Commit encoded output as the rendition and succeed the ticket."""
        with self.db.write_unit() as db:
            self.publishable(db, ticket_id)
            blob = self.blobs.put(path, db)
            artifact_id, created = identifier("artifact"), now()
            self.store.insert_rendition(db, ticket_id, artifact_id, blob, metadata, created)
            if not self.store.mark_succeeded(db, ticket_id, now()):
                raise LoomError("ticket_state", "Only an active render can commit", 409)
            self.db.event(db, ticket_id, "succeeded", {"artifact": artifact_id, "blob": blob["hash"]})
            return self.store.load_artifact(db, artifact_id)
