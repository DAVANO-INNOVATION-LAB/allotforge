import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat()


class Store:
    """Small persistent configuration store. Never a reservation ledger."""
    def __init__(self, path: str, seed=None, source=None):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pools (id TEXT PRIMARY KEY, body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS policies (id TEXT PRIMARY KEY, pool_id TEXT NOT NULL REFERENCES pools(id), body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, type TEXT NOT NULL, message TEXT NOT NULL);
        ''')
        if source:
            current = self.db.execute("SELECT value FROM meta WHERE key='source'").fetchone()
            if current and current[0] != source:
                self.db.close()
                raise ValueError("Database source mismatch; use a separate database for live and demo modes.")
            with self.db:
                self.db.execute("INSERT OR IGNORE INTO meta VALUES ('source',?)", (source,))
        with self.lock, self.db:
            if seed and not self.db.execute("SELECT 1 FROM meta WHERE key='seeded'").fetchone():
                for item in seed.get("pools", []):
                    self.db.execute("INSERT OR IGNORE INTO pools VALUES (?,?)", (item["id"], json.dumps(item)))
                for item in seed.get("policies", []):
                    self.db.execute("INSERT OR IGNORE INTO policies VALUES (?,?,?)", (item["id"], item["pool_id"], json.dumps(item)))
                self.db.execute("INSERT INTO meta VALUES ('seeded','true')")
                self._event("demo.initialized", "Loaded demo pool and policy configuration")

    def _event(self, kind, message):
        self.db.execute("INSERT INTO events(at,type,message) VALUES (?,?,?)", (now(), kind, message))
        self.db.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT 1000)")

    def list(self, kind):
        if kind not in ("pools", "policies"):
            raise ValueError("invalid collection")
        with self.lock:
            return [json.loads(r[0]) for r in self.db.execute(f"SELECT body FROM {kind} ORDER BY id")]

    def create(self, kind, body):
        with self.lock, self.db:
            if kind == "pools":
                self.db.execute("INSERT INTO pools VALUES (?,?)", (body["id"], json.dumps(body)))
            elif kind == "policies":
                if not self.db.execute("SELECT 1 FROM pools WHERE id=?", (body["pool_id"],)).fetchone():
                    raise KeyError("Pool not found")
                self.db.execute("INSERT INTO policies VALUES (?,?,?)", (body["id"], body["pool_id"], json.dumps(body)))
            else:
                raise ValueError("invalid collection")
            self._event(f"{kind[:-1] if kind == 'pools' else 'policy'}.created", f"Created {body['name']} (local advisory configuration)")
        return body

    def delete(self, kind, item_id):
        if kind not in ("pools", "policies"):
            raise ValueError("invalid collection")
        with self.lock, self.db:
            cur = self.db.execute(f"DELETE FROM {kind} WHERE id=?", (item_id,))
            if not cur.rowcount:
                raise KeyError("Configuration not found")
            self._event("configuration.deleted", f"Deleted local {kind} configuration: {item_id}")

    def events(self):
        with self.lock:
            return [dict(zip(("id", "at", "type", "message"), row)) for row in self.db.execute("SELECT id,at,type,message FROM events ORDER BY id DESC LIMIT 20")]

    def close(self):
        self.db.close()
