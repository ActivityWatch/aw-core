"""Recover a malformed SQLite file so aw-server can start instead of restart-looping.

Ubuntu/Debian sqlite3 is often built without ``SQLITE_ENABLE_DBPAGE_VTAB``, so
the shell ``.recover`` command fails with ``no such table: sqlite_dbpage``.
macOS sqlite3 usually has dbpage and ``.recover`` is the better tool (it
restored 148k events on a real corrupt ``peewee-sqlite.v2.db``).

This module tries, in order:

1. ``sqlite3 .recover`` when dbpage is available
2. ``sqlite3 .bail off .dump`` with ``ROLLBACK`` rewritten to ``COMMIT``
3. Pure-Python schema + row copy (Windows CI has no sqlite3 CLI)
4. Reconstruct any ``eventmodel.bucket_id`` rows missing from ``bucketmodel``

The original file is copied aside as ``<path>.corrupt-<UTC>`` before replacement.
Disable with ``AW_SQLITE_AUTO_RECOVER=0``.
"""

from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import stat
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
        _prepare_recovery_file(path, tmp_dest)
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
        _assert_recovered_schema(tmp_dest)
        _replace_live_db(path, tmp_dest)
    except Exception as exc:
        _remove_if_exists(tmp_dest)
        _restore_sidecars(path, sidecar)
        if isinstance(exc, SqliteRecoverError):
            raise SqliteRecoverError(
                f"Failed to recover {path} (original preserved at {sidecar}): {exc}\n"
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
    if sqlite_bin is not None:
        if _has_dbpage(sqlite_bin) and _recover_with_dbpage(sqlite_bin, src, dest):
            return True
        try:
            if _recover_with_dump(sqlite_bin, src, dest):
                return True
        except SqliteRecoverError as exc:
            logger.info("CLI dump recover failed, trying Python fallback: %s", exc)
            _truncate(dest)
    if _recover_with_python(src, dest):
        return True
    raise SqliteRecoverError(
        "sqlite3 CLI recover/dump failed or is missing, and the Python "
        "row-copy fallback produced no database"
    )


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
        _truncate(dest)
        return False
    if load.returncode != 0:
        logger.info("sqlite3 .recover load failed: %s", (load_err or "").strip()[:300])
        _truncate(dest)
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


def _ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _open_ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _recover_with_python(src: str, dest: str) -> bool:
    """Copy schema + surviving rows without the sqlite3 CLI.

    Used on Windows CI (no sqlite3.exe) and as a last resort when CLI recover
    fails. A poisoned connection is reopened after each DatabaseError.
    """
    _truncate(dest)
    src_con = _open_ro(src)
    dest_con = sqlite3.connect(dest)
    src_closed = False
    try:
        try:
            tables = src_con.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL"
            ).fetchall()
            indexes = src_con.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
            ).fetchall()
        except sqlite3.Error as exc:
            logger.info("Python recover cannot read sqlite_master: %s", exc)
            _truncate(dest)
            return False
        if not tables:
            _truncate(dest)
            return False
        for _name, sql in tables:
            dest_con.execute(sql)
        dest_con.commit()
        src_con.close()
        src_closed = True
        copied = 0
        for name, _sql in tables:
            copied += _copy_table_rows(src, dest_con, name)
        for (sql,) in indexes:
            try:
                dest_con.execute(sql)
            except sqlite3.Error:
                continue
        dest_con.commit()
        logger.info("Python recover copied %s row(s) from %s", copied, src)
        return os.path.exists(dest) and os.path.getsize(dest) > 0
    except sqlite3.Error as exc:
        logger.info("Python recover failed: %s", exc)
        _truncate(dest)
        return False
    finally:
        if not src_closed:
            src_con.close()
        try:
            dest_con.close()
        except sqlite3.Error:
            pass


def _copy_table_rows(src: str, dest_con: sqlite3.Connection, table: str) -> int:
    ident = _ident(table)
    src_con = _open_ro(src)
    copied = 0
    try:
        cols = [row[1] for row in src_con.execute(f"PRAGMA table_info({ident})")]
        if not cols:
            return 0
        col_list = ", ".join(_ident(c) for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        insert_sql = (
            f"INSERT OR IGNORE INTO {ident} ({col_list}) VALUES ({placeholders})"
        )
        pk_cols = [
            row[1] for row in src_con.execute(f"PRAGMA table_info({ident})") if row[5]
        ]
        try:
            rows = src_con.execute(f"SELECT {col_list} FROM {ident}").fetchall()
            dest_con.executemany(insert_sql, rows)
            dest_con.commit()
            return len(rows)
        except sqlite3.Error:
            src_con.close()
            src_con = _open_ro(src)
        if not pk_cols:
            return 0
        pk = pk_cols[0]
        pk_ident = _ident(pk)
        try:
            ids = [row[0] for row in src_con.execute(f"SELECT {pk_ident} FROM {ident}")]
        except sqlite3.Error:
            src_con.close()
            src_con = _open_ro(src)
            # Integer primary keys are the peewee/eventmodel case.
            try:
                mx = src_con.execute(f"SELECT max({pk_ident}) FROM {ident}").fetchone()
                ids = list(range(1, int(mx[0]) + 1)) if mx and mx[0] else []
            except sqlite3.Error:
                return 0
        for key in ids:
            try:
                row = src_con.execute(
                    f"SELECT {col_list} FROM {ident} WHERE {pk_ident}=?",
                    (key,),
                ).fetchone()
                if row:
                    dest_con.execute(insert_sql, row)
                    copied += 1
            except sqlite3.Error:
                src_con.close()
                src_con = _open_ro(src)
        dest_con.commit()
        return copied
    finally:
        src_con.close()


_BUCKETMODEL_DDL = (
    'CREATE TABLE "bucketmodel" ('
    '"key" INTEGER NOT NULL PRIMARY KEY, '
    '"id" VARCHAR(255) NOT NULL, '
    '"created" DATETIME NOT NULL, '
    '"name" VARCHAR(255), '
    '"type" VARCHAR(255) NOT NULL, '
    '"client" VARCHAR(255) NOT NULL, '
    '"hostname" VARCHAR(255) NOT NULL, '
    '"datastr" VARCHAR(255))'
)
_BUCKETMODEL_ID_INDEX_DDL = (
    'CREATE UNIQUE INDEX IF NOT EXISTS "bucketmodel_id" ON "bucketmodel" ("id")'
)


def _table_names(con: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _ensure_bucketmodel(con: sqlite3.Connection) -> None:
    """Create the peewee bucket catalog if a dump salvaged events but not buckets."""
    con.execute(_BUCKETMODEL_DDL)
    con.execute(_BUCKETMODEL_ID_INDEX_DDL)


def _reconstruct_missing_buckets(path: str) -> int:
    con = sqlite3.connect(path)
    try:
        tables = _table_names(con)
        if "eventmodel" not in tables:
            return 0
        if "bucketmodel" not in tables:
            _ensure_bucketmodel(con)
            con.commit()
            logger.warning(
                "Recovered file %s had eventmodel but no bucketmodel; "
                "created an empty bucket catalog before reconstructing keys",
                path,
            )
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


def _assert_recovered_schema(path: str) -> None:
    """Refuse to install a recovered file whose salvaged events would be unreachable.

    ``PRAGMA quick_check`` only validates pages. A dump can keep ``eventmodel``
    and omit ``bucketmodel``; peewee would then create an empty catalog and
    hide the recovered events.
    """
    con = sqlite3.connect(path)
    try:
        tables = _table_names(con)
        if "eventmodel" not in tables:
            return
        if "bucketmodel" not in tables:
            raise SqliteRecoverError("recovered file has eventmodel but no bucketmodel")
        missing = con.execute(
            """
            SELECT DISTINCT e.bucket_id
            FROM eventmodel e
            LEFT JOIN bucketmodel b ON b."key" = e.bucket_id
            WHERE b."key" IS NULL AND e.bucket_id IS NOT NULL
            """
        ).fetchall()
        if missing:
            raise SqliteRecoverError(
                "recovered file has events whose buckets could not be reconstructed"
            )
    except sqlite3.Error as exc:
        raise SqliteRecoverError(f"recovered file schema check failed: {exc}") from exc
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


def _prepare_recovery_file(src: str, dest: str) -> None:
    """Create an empty recovery file with the live database's permissions."""
    mode = stat.S_IMODE(os.stat(src).st_mode)
    fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        # POSIX umask may have removed bits requested above. Applying the exact
        # original mode before recovery starts also makes permission failures
        # fail closed, before any activity data is written. Windows has no
        # fchmod and does not preserve POSIX permission bits.
        if os.name != "nt":
            os.fchmod(fd, mode)  # type: ignore[attr-defined]
    finally:
        os.close(fd)


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


def _truncate(path: str) -> None:
    with open(path, "wb"):
        pass


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
