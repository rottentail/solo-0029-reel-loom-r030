"""SQLite adapter for render tickets and their renditions.

This module records what the lifecycle service tells it to record. It never
decides whether a ticket may start, cancel, fail, retry or publish, and it does
not resolve reels, looks or source media; those domain capabilities belong to
the application services. Callers own the surrounding write transaction.
"""

import json
from .errors import missing
from .values import canonical


class TicketStore:
    def __init__(self, db):
        self.db = db

    # -- write side ---------------------------------------------------------

    def insert_queued(self, db, ticket_id, snapshot, created, event_kind, event_payload):
        db.execute("INSERT INTO tickets(id,reel,revision,look_id,look_version,snapshot,fingerprint,state,attempt,created,updated) "
                   "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                   (ticket_id, snapshot["reel"], snapshot["revision"], snapshot["look"]["id"],
                    snapshot["look"]["version"], canonical(snapshot), snapshot["fingerprint"],
                    "queued", 1, created, created))
        self.db.event(db, ticket_id, event_kind, event_payload)

    def mark_started(self, db, ticket_id, updated):
        return db.execute("UPDATE tickets SET state='running',updated=? WHERE id=? AND state='queued'",
                          (updated, ticket_id)).rowcount == 1

    def request_cancel(self, db, ticket_id, updated):
        db.execute("UPDATE tickets SET cancel=1,updated=? WHERE id=?", (updated, ticket_id))

    def mark_cancelled(self, db, ticket_id, updated):
        db.execute("UPDATE tickets SET state='cancelled',cancel=1,updated=? WHERE id=? AND state='queued'",
                   (updated, ticket_id))

    def mark_failed(self, db, ticket_id, state, error, updated):
        db.execute("UPDATE tickets SET state=?,error=?,updated=? WHERE id=? AND state='running'",
                   (state, canonical(error), updated, ticket_id))

    def mark_succeeded(self, db, ticket_id, updated):
        return db.execute("UPDATE tickets SET state='succeeded',progress=1,error=NULL,updated=? "
                          "WHERE id=? AND state='running'", (updated, ticket_id)).rowcount == 1

    def mark_recovered(self, db, ticket_id, state, error, updated):
        db.execute("UPDATE tickets SET state=?,error=?,updated=? WHERE id=? AND state='running'",
                   (state, canonical(error), updated, ticket_id))

    def requeue(self, db, ticket_id, attempt, updated):
        return db.execute("UPDATE tickets SET state='queued',attempt=?,progress=0,cancel=0,error=NULL,updated=? "
                          "WHERE id=? AND state IN ('failed','cancelled')", (attempt, updated, ticket_id)).rowcount == 1

    def set_progress(self, db, ticket_id, value, updated):
        db.execute("UPDATE tickets SET progress=MAX(progress,?),updated=? WHERE id=? AND state='running'",
                   (value, updated, ticket_id))

    def insert_rendition(self, db, ticket_id, artifact_id, blob, metadata, created):
        db.execute("INSERT INTO artifacts VALUES(?,?,?,?,?,?,?)",
                   (artifact_id, ticket_id, None, "rendition", blob["hash"], canonical(metadata), created))

    # -- read side ----------------------------------------------------------

    def row(self, db, ticket_id):
        row = db.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
        if row is None:
            raise missing("render ticket", ticket_id)
        return dict(row)

    def publish_state(self, db, ticket_id):
        """Report the state a publication transaction must honour, or None."""
        row = db.execute("SELECT state,cancel FROM tickets WHERE id=?", (ticket_id,)).fetchone()
        return None if row is None else (row["state"], bool(row["cancel"]))

    def cancellation_flag(self, db, ticket_id):
        """Report the stored cancel flag, or None when the ticket is absent."""
        row = db.execute("SELECT cancel FROM tickets WHERE id=?", (ticket_id,)).fetchone()
        return None if row is None else bool(row["cancel"])

    def load(self, db, ticket_id):
        result = self.row(db, ticket_id)
        result["snapshot"] = json.loads(result["snapshot"])
        result["error"] = json.loads(result["error"]) if result["error"] else None
        result["cancel"] = bool(result["cancel"])
        result["artifacts"] = [dict(item) for item in db.execute(
            "SELECT id,kind,blob,metadata FROM artifacts WHERE ticket=? ORDER BY created,id", (ticket_id,))]
        for item in result["artifacts"]:
            item["metadata"] = json.loads(item["metadata"])
        return result

    def load_artifact(self, db, artifact_id):
        row = db.execute("SELECT * FROM artifacts WHERE id=?", (artifact_id,)).fetchone()
        if row is None:
            raise missing("artifact", artifact_id)
        return {**dict(row), "metadata": json.loads(row["metadata"])}

    def ids(self, db, state=None, limit=100):
        if state:
            rows = db.execute("SELECT id FROM tickets WHERE state=? ORDER BY created DESC,id LIMIT ?",
                              (state, limit))
        else:
            rows = db.execute("SELECT id FROM tickets ORDER BY created DESC,id LIMIT ?", (limit,))
        return [row["id"] for row in rows]

    def next_queued(self, db, order):
        clause = ", ".join(order)
        row = db.execute(f"SELECT id FROM tickets WHERE state='queued' ORDER BY {clause} LIMIT 1").fetchone()
        return None if row is None else row["id"]

    def running(self, db):
        return [dict(row) for row in db.execute("SELECT id,state,cancel FROM tickets WHERE state='running'")]
