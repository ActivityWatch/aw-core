"""Recover a malformed SQLite file so aw-server can start instead of restart-looping.

Ubuntu/Debian sqlite3 is often built without ``SQLITE_ENABLE_DBPAGE_VTAB``, so
the shell ``.recover`` command fails with ``no such table: sqlite_dbpage``.
macOS sqlite3 usually has dbpage and ``.recover`` is the better tool (it
restored 148k events on a real corrupt ``peewee-sqlite.v2.db``).

This module tries, in order:

1. ``sqlite3 .recover`` when dbpage is available
2. ``sqlite3 .bail off .dump`` with ``ROLLBACK`` rewritten to ``COMMIT``
3. Reconstruct any ``eventmodel.bucket_id`` rows missing from ``bucketmodel``

The original file is copied aside as ``<path>.corrupt-<UTC>`` before replacement.
Disable with ``AW_SQLITE_AUTO_RECOVER=0``.
"""

from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

AUTO_RECOVER_ENV = "AW_SQLITE_AUTO_RECOVER"
RECOVER_TIMEOUT_SEC = 300


class SqliteRecoverError(RuntimeError):
    """Raised when the database is malformed and recovery did not succeed."""


def auto_recover_enabled() -> bool:
    val = os.environ.get(AUTO_RECOVER_ENV, "1").strip().lower()
    return val not in {"0", "false", "no", "off"}


def is_sqlite_healthy(path: str) -> bool:
    """Return True if path is missing (will be created) or PRAGMA quick_check is ok."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return True
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = con.execute("PRAGMA quick_check").fetchone()
            return bool(row) and str(row[0]).lower() == "ok"
        finally:
            con.close()
    except sqlite3.Error:
        return False


def maybe_recover_malformed_sqlite(path: str) -> str | None:
    """If ``path`` is malformed, preserve it and replace with a recovered copy.

    Returns the sidecar path when recovery ran, otherwise None.
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    if is_sqlite_healthy(path):
        return None
    if not auto_recover_enabled():
        raise SqliteRecoverError(
            _manual_instructions(path)
            + f"\nAuto-recovery disabled via {AUTO_RECOVER_ENV}=0."
        )
    sidecar = _copy_aside(path)
    logger.warning(
        "SQLite database %s is malformed; original copied to %s. Attempting recovery.",
        path,
        sidecar,
    )
    tmp_dest = path + ".recovered-tmp"
    _remove_if_exists(tmp_dest)
    try:
        recovered = _try_recover(sidecar, tmp_dest)
        if (
            not recovered
            or not os.path.exists(tmp_dest)
            or os.path.getsize(tmp_dest) == 0
        ):
            raise SqliteRecoverError("recovery produced no database file")
        _reconstruct_missing_buckets(tmp_dest)
        if not is_sqlite_healthy(tmp_dest):
            raise SqliteRecoverError("recovered file still fails PRAGMA quick_check")
        _replace_live_db(path, tmp_dest)
    except Exception as exc:
        _remove_if_exists(tmp_dest)
        _restore_sidecars(path, sidecar)
        if isinstance(exc, SqliteRecoverError):
            raise SqliteRecoverError(
                f"Failed to recover {path} (original preserved at {sidecar}).\n"
                + _manual_instructions(sidecar)
            ) from exc
        raise SqliteRecoverError(
            f"Failed to recover {path} (original preserved at {sidecar}): {exc}\n"
            + _manual_instructions(sidecar)
        ) from exc
    event_count, bucket_count = _counts(path)
    logger.warning(
        "SQLite recovery succeeded for %s (%s events, %s buckets). Original at %s.",
        path,
        event_count,
        bucket_count,
        sidecar,
    )
    return sidecar


def sanitize_dump_sql(sql: str) -> str:
    """Turn a ``.dump`` of a corrupt DB into SQL that can be loaded."""
    lines: list[str] = []
    for line in sql.splitlines():
        if "CORRUPTION ERROR" in line:
            continue
        if line.startswith("ROLLBACK;"):
            lines.append("COMMIT;")
            continue
        lines.append(line)
    return "\n".join(lines) + "\n"


def _sqlite_bin() -> str | None:
    return shutil.which("sqlite3")


def _has_dbpage(sqlite_bin: str) -> bool:
    try:
        proc = subprocess.run(
            [
                sqlite_bin,
                ":memory:",
                "CREATE VIRTUAL TABLE temp.t USING sqlite_dbpage;",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _try_recover(src: str, dest: str) -> bool:
    sqlite_bin = _sqlite_bin()
    if sqlite_bin is None:
        raise SqliteRecoverError("sqlite3 CLI not found on PATH; cannot auto-recover.")
    if _has_dbpage(sqlite_bin) and _recover_with_dbpage(sqlite_bin, src, dest):
        return True
    return _recover_with_dump(sqlite_bin, src, dest)


def _recover_with_dbpage(sqlite_bin: str, src: str, dest: str) -> bool:
    dump = subprocess.Popen(
        [sqlite_bin, src, ".recover"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    load = subprocess.Popen(
        [sqlite_bin, dest],
        stdin=dump.stdout,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if dump.stdout is not None:
        dump.stdout.close()
    try:
        _, load_err = load.communicate(timeout=RECOVER_TIMEOUT_SEC)
        _, dump_err = dump.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        dump.kill()
        load.kill()
        dump.communicate()
        load.communicate()
        logger.warning("sqlite3 .recover timed out")
        return False
    if dump.returncode != 0:
        logger.info(
            "sqlite3 .recover unavailable or failed (rc=%s): %s",
            dump.returncode,
            (dump_err or "").strip()[:300],
        )
        _remove_if_exists(dest)
        return False
    if load.returncode != 0:
        logger.info("sqlite3 .recover load failed: %s", (load_err or "").strip()[:300])
        _remove_if_exists(dest)
        return False
    return os.path.exists(dest) and os.path.getsize(dest) > 0


def _recover_with_dump(sqlite_bin: str, src: str, dest: str) -> bool:
    try:
        dump = subprocess.run(
            [sqlite_bin, src, ".bail off", ".dump"],
            capture_output=True,
            text=True,
            timeout=RECOVER_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired as exc:
        raise SqliteRecoverError("sqlite3 .dump timed out") from exc
    sql = sanitize_dump_sql(dump.stdout or "")
    if "CREATE TABLE" not in sql:
        raise SqliteRecoverError(
            "sqlite3 .dump produced no schema: " + (dump.stderr or "").strip()[:300]
        )
    try:
        load = subprocess.run(
            [sqlite_bin, dest],
            input=sql,
            capture_output=True,
            text=True,
            timeout=RECOVER_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired as exc:
        raise SqliteRecoverError("sqlite3 dump load timed out") from exc
    if load.returncode != 0:
        raise SqliteRecoverError(
            f"sqlite3 dump load failed (rc={load.returncode}): "
            + (load.stderr or "").strip()[:300]
        )
    if dump.returncode not in (0, 1):
        # rc=1 is common on corrupt dumps; rc=0 also happens with .bail off.
        logger.info(
            "sqlite3 .dump rc=%s stderr=%s",
            dump.returncode,
            (dump.stderr or "").strip()[:200],
        )
    return os.path.exists(dest) and os.path.getsize(dest) > 0


def _reconstruct_missing_buckets(path: str) -> int:
    con = sqlite3.connect(path)
    try:
        tables = {
            row[0]
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "eventmodel" not in tables or "bucketmodel" not in tables:
            return 0
        missing = con.execute(
            """
            SELECT DISTINCT e.bucket_id
            FROM eventmodel e
            LEFT JOIN bucketmodel b ON b."key" = e.bucket_id
            WHERE b."key" IS NULL AND e.bucket_id IS NOT NULL
            """
        ).fetchall()
        now = datetime.now(timezone.utc).isoformat()
        n = 0
        for (key,) in missing:
            con.execute(
                """
                INSERT INTO bucketmodel
                    ("key", id, created, name, type, client, hostname, datastr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    f"recovered-{key}",
                    now,
                    "recovered",
                    "unknown",
                    "sqlite-recover",
                    "unknown",
                    "{}",
                ),
            )
            n += 1
        if n:
            con.commit()
            logger.warning(
                "Reconstructed %s missing bucket row(s) as recovered-<key> in %s",
                n,
                path,
            )
        return n
    finally:
        con.close()


def _counts(path: str) -> tuple[str, str]:
    try:
        con = sqlite3.connect(path)
        try:
            tables = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            events = (
                con.execute("SELECT count(*) FROM eventmodel").fetchone()[0]
                if "eventmodel" in tables
                else "n/a"
            )
            buckets = (
                con.execute("SELECT count(*) FROM bucketmodel").fetchone()[0]
                if "bucketmodel" in tables
                else "n/a"
            )
            return str(events), str(buckets)
        finally:
            con.close()
    except sqlite3.Error:
        return "n/a", "n/a"


def _copy_aside(path: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sidecar = f"{path}.corrupt-{ts}"
    shutil.copy2(path, sidecar)
    for suffix in ("-wal", "-shm", "-journal"):
        extra = path + suffix
        if os.path.exists(extra) and os.path.getsize(extra) > 0:
            shutil.copy2(extra, sidecar + suffix)
    return sidecar


def _replace_live_db(path: str, recovered: str) -> None:
    # Drop WAL/SHM first so SQLite cannot apply the old log to the new file.
    for suffix in ("-wal", "-shm", "-journal"):
        _remove_if_exists(path + suffix)
    os.replace(recovered, path)


def _restore_sidecars(path: str, sidecar: str) -> None:
    """Put WAL/SHM back if replacement failed after they were deleted."""
    for suffix in ("-wal", "-shm", "-journal"):
        src = sidecar + suffix
        dest = path + suffix
        if os.path.exists(src) and not os.path.exists(dest):
            shutil.copy2(src, dest)


def _remove_if_exists(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        return


def _manual_instructions(path: str) -> str:
    return (
        "SQLite database is malformed. Preserve the file and recover with:\n"
        f"  sqlite3 {path} '.recover' | sqlite3 recovered.db\n"
        "If .recover fails with 'no such table: sqlite_dbpage', use:\n"
        f"  sqlite3 {path} '.bail off' '.dump' | sed '/CORRUPTION ERROR/d; s/^ROLLBACK;/COMMIT;/'"
        " | sqlite3 recovered.db\n"
        "Then replace the original database with recovered.db."
    )
