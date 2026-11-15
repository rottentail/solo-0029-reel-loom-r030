import json
from .errors import require, missing
from .values import identifier, now, canonical


class Artifacts:
    def __init__(self, db, blobs):
        self.db, self.blobs = db, blobs

    def add(self, asset_id, kind, path, metadata):
        with self.db.write_unit() as db:
            row = db.execute("SELECT state FROM assets WHERE id=?", (asset_id,)).fetchone()
            require(row is not None and row["state"] == "ready", "asset_retired", "Source is no longer available", 409)
            blob = self.blobs.put(path, db)
            artifact_id = identifier("artifact")
            db.execute("INSERT INTO artifacts VALUES(?,?,?,?,?,?,?)",
                       (artifact_id, None, asset_id, kind, blob["hash"], canonical(metadata), now()))
            self.db.event(db, asset_id, "artifact_created", {"artifact": artifact_id, "kind": kind})
            return self.get(artifact_id, db)

    def get(self, artifact_id, db=None):
        if db is None:
            with self.db.read() as read:
                return self.get(artifact_id, read)
        row = db.execute("SELECT * FROM artifacts WHERE id=?", (artifact_id,)).fetchone()
        if row is None:
            raise missing("artifact", artifact_id)
        return {**dict(row), "metadata": json.loads(row["metadata"])}

    def list_for_asset(self, asset_id):
        with self.db.read() as db:
            return [self.get(row["id"], db) for row in db.execute("SELECT id FROM artifacts WHERE asset=? ORDER BY created,id", (asset_id,))]
