#!/usr/bin/env python3
"""
File Scanner Script - Scans folders, tracks files in SQLite, supports
parallel MD5 computation, duplicate-copy limits, and validation reporting.
"""

import argparse
import hashlib
import os
import sqlite3
import sys
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, NamedTuple, Optional, Tuple


class FileMetadata(NamedTuple):
    """File metadata from filesystem scan."""
    filename: bytes
    full_path: bytes
    top_folder: bytes
    mtime: float
    filesize: int


class FileRecord(NamedTuple):
    """File record from database."""
    filename: bytes
    full_path: bytes
    top_folder: bytes
    mtime: float
    md5: str | None
    filesize: int


class LimitCheckResult(NamedTuple):
    """Result of a limit check query."""
    full_path: bytes
    md5: str
    copies: int


class ValidationResult(NamedTuple):
    """Result of validation for a file."""
    top_folder: bytes
    full_path: bytes
    expected_md5: Optional[str]
    actual_md5: str | None = None


def compute_md5(filepath: bytes) -> str:
    """Compute MD5 hash of a file, reading in chunks."""
    hasher = hashlib.md5()
    try:
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4 * 1024 * 1024), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    except (IOError, OSError):
        return ""


def compute_md5_wrapper(filepath_bytes: bytes) -> str:
    """Wrapper for multiprocessing.
    Accepts a single file path (bytes) and returns its MD5 hash.
    """
    res = compute_md5(filepath_bytes)
    print(f"[-] Computed MD5 for {to_printable(filepath_bytes)}")
    return res


def get_file_stat(filepath: bytes) -> Optional[Tuple[float, float]]:
    """Get mtime and filesize for a file path.

    Returns ``None`` if the file cannot be accessed.
    """
    try:
        stat = os.stat(filepath)
        return stat.st_mtime, stat.st_size
    except (IOError, OSError):
        return None


def scan_folder(top_folder: str) -> List[FileMetadata]:
    """
    Recursively scan a folder and yield file metadata.
    Returns a list of FileMetadata with: filename, full_path (relative to
    top_folder), top_folder, mtime, filesize. All paths are stored as
    bytes to handle non-UTF8 filenames.
    """
    results: list[FileMetadata] = []
    top_normalized = os.path.normpath(top_folder)
    top_bytes = top_normalized.encode(errors='surrogateescape')

    # Use os.walk to traverse the directory tree
    for dirpath, _, filenames in os.walk(top_folder, followlinks=False):
        for filename in filenames:
            full_path_abs = os.path.join(dirpath, filename)
            # Compute relative path from top_folder
            rel_path = os.path.relpath(full_path_abs, top_normalized)
            try:
                stat = os.stat(full_path_abs)
                mtime = stat.st_mtime
                filesize = stat.st_size
            except (IOError, OSError):
                continue

            filename_bytes = filename.encode(errors='surrogateescape')
            rel_path_bytes = rel_path.encode(errors='surrogateescape')
            results.append(FileMetadata(
                filename=filename_bytes,
                full_path=rel_path_bytes,
                top_folder=top_bytes,
                mtime=mtime,
                filesize=filesize,
            ))

    return results


class FileDB:
    """Handles SQLite database operations for file tracking."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.ensure_table()

    def ensure_table(self) -> None:
        """Create the files table if it doesn't exist."""
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS files (
                filename BLOB NOT NULL,
                full_path BLOB NOT NULL,
                top_folder BLOB NOT NULL,
                mtime REAL NOT NULL,
                md5 TEXT,
                filesize INTEGER NOT NULL,
                PRIMARY KEY (top_folder, full_path)
            )
        ''')
        self.conn.commit()

    def load_all(self) -> Dict[Tuple[bytes, bytes], FileRecord]:
        """Load all rows from the database, keyed by (top_folder, full_path).

        Returns dict keyed by (top_folder, full_path).
        """
        cursor = self.conn.execute(
            'SELECT filename, full_path, top_folder, mtime, md5, filesize '
            'FROM files'
        )
        result: dict[tuple[bytes, bytes], FileRecord] = {}
        for row in cursor:
            filename, full_path, top_folder, mtime, md5, filesize = row
            key = (top_folder, full_path)
            result[key] = FileRecord(
                filename=filename,
                full_path=full_path,
                top_folder=top_folder,
                mtime=mtime,
                md5=md5,
                filesize=filesize,
            )
        return result

    def insert_rows(self, rows: List[FileMetadata]) -> None:
        """Insert multiple rows into the database."""
        for row in rows:
            self.conn.execute(
                '''INSERT INTO files (filename, full_path, top_folder,
                   mtime, md5, filesize) VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(top_folder, full_path) DO UPDATE SET
                       filename=excluded.filename,
                       top_folder=excluded.top_folder,
                       mtime=excluded.mtime,
                       md5=excluded.md5,
                       filesize=excluded.filesize''',
                (row.filename, row.full_path, row.top_folder,
                 row.mtime, None, row.filesize)
            )
        self.conn.commit()

    def update_rows(self, rows: List[FileMetadata]) -> None:
        """Update multiple rows in the database."""
        for row in rows:
            self.conn.execute(
                '''UPDATE files SET filename=?, mtime=?, md5=?,
                   filesize=? WHERE top_folder=? AND full_path=?''',
                (row.filename, row.mtime,
                 None, row.filesize, row.top_folder,
                 row.full_path)
            )
        self.conn.commit()

    def delete_paths(self, paths: List[Tuple[bytes, bytes]]) -> None:
        """Delete rows by (top_folder, full_path) tuples."""
        for top_folder, full_path in paths:
            self.conn.execute(
                'DELETE FROM files WHERE top_folder = ? AND full_path = ?',
                (top_folder, full_path)
            )
        self.conn.commit()

    def query_limit(self, limit: int) -> List[LimitCheckResult]:
        """
        Find (full_path, md5) pairs that appear in fewer than `limit`
        distinct top_folders. Returns list of LimitCheckResult.
        """
        cursor = self.conn.execute('''
            SELECT full_path, md5, COUNT(DISTINCT top_folder) AS copies
            FROM files
            GROUP BY full_path, md5
            HAVING copies < ?
        ''', (limit,))
        return [LimitCheckResult(row[0], row[1], row[2]) for row in cursor]

    def get_rows_for_validation(self, top_folder: Optional[str] = None) -> List[tuple]:
        """Get rows to validate, optionally filtered by top_folder."""
        if top_folder is None:
            cursor = self.conn.execute(
                'SELECT filename, full_path, top_folder, mtime, md5, '
                'filesize FROM files'
            )
        elif top_folder == 'all':
            cursor = self.conn.execute(
                'SELECT filename, full_path, top_folder, mtime, md5, '
                'filesize FROM files'
            )
        else:
            top_normalized = os.path.normpath(top_folder)
            top_bytes = top_normalized.encode(errors='surrogateescape')
            cursor = self.conn.execute(
                'SELECT filename, full_path, top_folder, mtime, md5, '
                'filesize FROM files WHERE top_folder = ?',
                (top_bytes,)
            )
        return cursor.fetchall()

    def close(self) -> None:
        self.conn.close()


def to_printable(data: bytes) -> str:
    """Convert bytes to a printable string, replacing non-printable chars."""
    if isinstance(data, str):
        return data
    return data.decode(errors='ignore')


def perform_sync(db: FileDB, top_folder: str, ncores: int) -> None:
    """
    Perform synchronization between filesystem and database.
    - Insert new files
    - Update changed files (mtime or filesize changed)
    - Delete missing files
    """
    top_normalized = os.path.normpath(top_folder)
    top_bytes = top_normalized.encode(errors='surrogateescape')

    # Load existing DB data
    db_data = db.load_all()

    # Scan filesystem
    fs_data = scan_folder(top_folder)
    # Map full_path to FileMetadata objects
    fs_paths = {item.full_path: item for item in fs_data}

    to_insert = []
    to_update = []
    to_delete = []

    # Find new and changed files
    for item in fs_data:
        full_path = item.full_path
        key = (top_bytes, full_path)
        if key not in db_data:
            to_insert.append(item)
        else:
            db_row = db_data[key]
            mtime_changed = db_row.mtime != item.mtime
            size_changed = db_row.filesize != item.filesize
            if mtime_changed or size_changed:
                to_update.append(item)

    # Find deleted files - only for this top_folder
    for (tf, full_path) in db_data:
        if tf == top_bytes and full_path not in fs_paths:
            to_delete.append((tf, full_path))

    # Compute MD5s for inserts - need absolute paths
    if to_insert:
        # Build absolute paths for MD5 computation (list of bytes) and map them back to items
        paths_to_hash: List[bytes] = []
        path_to_item: Dict[bytes, FileMetadata] = {}
        for item in to_insert:
            rel_path_str = to_printable(item.full_path)
            abs_path = os.path.join(top_normalized, rel_path_str)
            abs_path_bytes = abs_path.encode(errors='surrogateescape')
            paths_to_hash.append(abs_path_bytes)
            path_to_item[abs_path_bytes] = item
        # Compute MD5s and insert rows with the computed values
        md5s = compute_md5_parallel(paths_to_hash, ncores)
        for abs_path, md5 in md5s.items():
            item = path_to_item[abs_path]
            db.conn.execute(
                '''INSERT INTO files (filename, full_path, top_folder, mtime, md5, filesize)\
                   VALUES (?, ?, ?, ?, ?, ?)\
                   ON CONFLICT(top_folder, full_path) DO UPDATE SET\
                       filename=excluded.filename,\
                       top_folder=excluded.top_folder,\
                       mtime=excluded.mtime,\
                       md5=excluded.md5,\
                       filesize=excluded.filesize''',
                (item.filename, item.full_path, item.top_folder, item.mtime, md5, item.filesize),
            )
        db.conn.commit()

    # Compute MD5s for updates - need absolute paths
    if to_update:
        paths_to_hash: List[bytes] = []
        path_to_item: Dict[bytes, FileMetadata] = {}
        for item in to_update:
            rel_path_str = to_printable(item.full_path)
            abs_path = os.path.join(top_normalized, rel_path_str)
            abs_path_bytes = abs_path.encode(errors='surrogateescape')
            paths_to_hash.append(abs_path_bytes)
            path_to_item[abs_path_bytes] = item
        md5s = compute_md5_parallel(paths_to_hash, ncores)
        for abs_path, md5 in md5s.items():
            item = path_to_item[abs_path]
            db.conn.execute(
                '''UPDATE files SET filename=?, mtime=?, md5=?, filesize=?\
                   WHERE top_folder=? AND full_path=?''',
                (item.filename, item.mtime, md5, item.filesize, item.top_folder, item.full_path),
            )
        db.conn.commit()

    # Delete missing files
    if to_delete:
        for top_folder_bytes, full_path_bytes in to_delete:
            print(f"[-] Deleted (missing): {to_printable(full_path_bytes)}")
        db.delete_paths(to_delete)

    # Print summary
    msg = f"[-] Sync complete: {len(to_insert)} inserted, "
    msg += f"{len(to_update)} updated, {len(to_delete)} deleted"
    print(f"{msg}")


def compute_md5_parallel(paths: List[bytes], ncores: int) -> Dict[bytes, str]:
    """Compute MD5 hashes for multiple files in parallel.

    Args:
        paths: List of file paths as bytes.
        ncores: Number of worker processes.
    Returns:
        Mapping of file path (bytes) to MD5 hex digest.
    """
    if not paths:
        return {}

    with ProcessPoolExecutor(max_workers=ncores) as executor:
        results = list(executor.map(compute_md5_wrapper, paths))

    return {path: md5 for path, md5 in zip(paths, results)}


def run_limit_check(db: FileDB, limit: int, report_path: str) -> None:
    """
    Run the limit check and write results to report.log.
    Each line: <full_path>#@#<existing_copy_count>
    (Note: report_path is where the report is written)
    """
    results = db.query_limit(limit)

    with open(report_path, 'w', encoding='utf-8',
              errors='surrogateescape') as f:
        for full_path, md5, copies in results:
            path_str = to_printable(full_path)
            f.write(f"{path_str}#@#{copies}\n")


def run_validation(db: FileDB, target: str, report_path: str, ncores: int) -> None:
    """
    Validate DB rows against filesystem.
    Generate report with MATCH, MISMATCH, MISSING, NEW sections.
    """
    rows = db.get_rows_for_validation(target)

    # Build set of DB paths keyed by (top_folder, full_path) with expected MD5s
    db_data = {}
    for row in rows:
        filename, full_path, top_folder, mtime, expected_md5, filesize = row
        key = (top_folder, full_path)
        db_data[key] = expected_md5

    # Scan filesystem for the target folder(s)
    if target == 'all':
        # Scan all folders - get unique top_folders from DB rows
        top_folders = set(row[2] for row in rows)
        fs_data = []
        for tf in top_folders:
            fs_data.extend(scan_folder(to_printable(tf)))
    else:
        fs_data = scan_folder(target)

    # Build filesystem lookup by (top_folder, full_path)
    fs_data_lookup = {}
    for item in fs_data:
        key = (item.top_folder, item.full_path)
        fs_data_lookup[key] = item

    # Compute MD5s for existing files in parallel - need absolute paths
    paths_to_hash: List[bytes] = []
    rel_path_mapping: Dict[bytes, bytes] = {}  # abs_path_bytes -> rel_path_bytes
    for item in fs_data:
        key = (item.top_folder, item.full_path)
        if key in db_data:
            top_folder_bytes = item.top_folder
            rel_path_bytes = item.full_path
            top_folder_str = to_printable(top_folder_bytes)
            rel_path_str = to_printable(rel_path_bytes)
            abs_path = os.path.join(top_folder_str, rel_path_str)
            abs_path_bytes = abs_path.encode(errors='surrogateescape')
            paths_to_hash.append(abs_path_bytes)
            rel_path_mapping[abs_path_bytes] = rel_path_bytes

    computed_md5s_abs = compute_md5_parallel(paths_to_hash, ncores)
    # Convert to relative path keys
    computed_md5s = {rel_path_mapping[ak]: md5
                     for ak, md5 in computed_md5s_abs.items()}

    match = []
    mismatch = []
    missing = []
    new_files = []

    # Check DB rows
    for key, expected_md5 in db_data.items():
        top_folder, full_path = key
        if key not in fs_data_lookup:
            missing.append((top_folder, full_path, expected_md5))
        else:
            actual_md5 = computed_md5s.get(full_path, '')
            if actual_md5 == expected_md5:
                match.append((top_folder, full_path, expected_md5))
            else:
                mismatch.append((top_folder, full_path, expected_md5,
                                 actual_md5))

    # Find new files (in FS but not in DB)
    for key in fs_data_lookup:
        if key not in db_data:
            top_folder, full_path = key
            new_files.append((top_folder, full_path))

    # Write report – only include sections that have entries
    with open(report_path, 'w', encoding='utf-8',
              errors='surrogateescape') as f:
        if match:
            f.write("=== MATCH ===\n")
            for top_folder, path, md5 in match:
                tf_str = to_printable(top_folder)
                p_str = to_printable(path)
                f.write(f"MATCH: {tf_str}/{p_str} (md5={md5})\n")
            f.write("\n")

        if mismatch:
            f.write("=== MISMATCH ===\n")
            for top_folder, path, expected, actual in mismatch:
                tf_str = to_printable(top_folder)
                p_str = to_printable(path)
                f.write(f"MISMATCH: {tf_str}/{p_str} (expected={expected}, "
                        f"actual={actual})\n")
            f.write("\n")

        if missing:
            f.write("=== MISSING ===\n")
            for top_folder, path, expected in missing:
                tf_str = to_printable(top_folder)
                p_str = to_printable(path)
                f.write(f"MISSING: {tf_str}/{p_str} (expected_md5={expected})\n")
            f.write("\n")

        if new_files:
            f.write("=== NEW ===\n")
            for top_folder, path in new_files:
                f.write(f"NEW: {to_printable(top_folder)}/{to_printable(path)}\n")


def parse_args() -> Tuple[argparse.ArgumentParser, argparse.Namespace]:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=('File scanner with SQLite tracking, parallel MD5, '
                     'and validation.')
    )
    # Allow zero or more top folders. Most commands use only the first folder,
    # but the limit check can accept multiple folders to index before checking.
    parser.add_argument('top_folder', nargs='*', help='Top folder(s) to scan')
    parser.add_argument('-n', '--ncores', type=int, default=None,
                        help='Number of cores for parallel MD5 computation '
                        '(default: all available)')
    parser.add_argument('-l', '--limit', type=int, default=None,
                        help='Check that each (full_path, md5) appears in '
                        'at least N distinct top_folders')
    parser.add_argument('-v', '--validate', nargs='?', const='all',
                        default=None,
                        help='Validate DB rows against filesystem. '
                        'Optional arg: top_folder or "all"')
    parser.add_argument('--db', type=str, default='files.db',
                        help='Path to SQLite database '
                        '(default: files.db in current directory)')
    parser.add_argument('--report', type=str, default='report.log',
                        help='Path to report file '
                        '(default: report.log in current directory)')

    return parser, parser.parse_args()


def main() -> None:
    parser, args = parse_args()

    # Determine number of cores
    if args.ncores is not None and args.ncores > 0:
        ncores = args.ncores
    else:
        ncores = os.cpu_count() or 1

    # Create/open database
    db = FileDB(args.db)

    try:
        if args.validate is not None:
            # Validation mode – use the first folder if any are supplied
            target = args.validate
            run_validation(db, target, args.report, ncores)
            print(f"[-] Validation complete. Report written to {args.report}")
        elif args.limit is not None:
            # Limit check – optionally sync one or more folders before checking
            if args.top_folder:
                for folder in args.top_folder:
                    perform_sync(db, folder, ncores)
            run_limit_check(db, args.limit, args.report)
            print(f"[-] Limit check complete. Report written to {args.report}")
        elif args.top_folder:
            # Normal sync mode – use the first provided folder
            perform_sync(db, args.top_folder[0], ncores)
            print(f"[-] DB sync complete for {args.top_folder[0]}")
        else:
            parser.print_help()
            sys.exit(1)
    finally:
        db.close()


if __name__ == '__main__':
    main()
