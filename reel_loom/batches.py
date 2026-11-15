"""Render-matrix read model.

Enqueue is owned by the ticket lifecycle service (`Tickets.submit_matrix`,
one transaction for the whole matrix). This module only aggregates the
independently executed member tickets for reads.
"""

import json
from . import ticket_rules
from .errors import require


class RenderBatches:
    def __init__(self, db, tickets):
        self.db, self.tickets = db, tickets

    def submit(self, body, key=None):
        return self.tickets.submit_matrix(body, key)

    def get(self, batch_id):
        with self.db.read() as db:
            row = db.execute("SELECT payload FROM events WHERE subject=? AND kind='matrix_submitted' "
                             "ORDER BY sequence LIMIT 1", (batch_id,)).fetchone()
            require(row is not None, "not_found", "Render matrix does not exist", 404)
            ids = json.loads(row["payload"])["tickets"]
            tickets = [self.tickets.get(x, db) for x in ids]
            return {"id": batch_id, "state": ticket_rules.batch_state([x["state"] for x in tickets]),
                    "tickets": tickets,
                    "progress": sum(x["progress"] for x in tickets)/len(tickets)}
