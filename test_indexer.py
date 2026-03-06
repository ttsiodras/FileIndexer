#!/usr/bin/env python3
"""Test suite for ``indexer.py`` based on the steps described in ``TEST.md``.

The script creates temporary directories, runs the indexer with the appropriate
options and asserts the expected state of the SQLite database and the generated
``report.log``.
"""

import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

INDEXER = Path(__file__).with_name("indexer.py")


def run_indexer(args, cwd=None):
    """Run ``indexer.py`` with the given *args* and return the completed process.

    ``cwd`` defaults to the current working directory; a temporary directory is
    used for isolation in the test suite.
    """
    return subprocess.run(
        [sys.executable, str(INDEXER)] + args,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def query_db(db_path: Path):
    """Return a list of rows (full_path, md5) from the ``files`` table."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.execute("SELECT full_path, md5 FROM files")
    rows = [(bytes(fp), md5) for fp, md5 in cur.fetchall()]
    conn.close()
    return rows


def read_report(report_path: Path) -> str:
    return report_path.read_text(encoding="utf-8", errors="ignore")


def assert_match(report: str, pattern: str):
    if pattern not in report:
        raise AssertionError(f"Expected pattern not found in report: {pattern!r}")


def main():
    # Use a temporary directory as the working directory for all tests.
    with tempfile.TemporaryDirectory() as tmpdir:
        work = Path(tmpdir)
        db_path = work / "test.db"
        report_path = work / "report.log"

        # Helper to clean DB and report between runs.
        def clean():
            if db_path.exists():
                db_path.unlink()
            if report_path.exists():
                report_path.unlink()

        # ---------- Test 1: add two files in an empty folder ----------
        clean()
        folder = work / "folder1"
        folder.mkdir()
        (folder / "a.txt").write_text("hello")
        (folder / "b.txt").write_text("world")
        proc = run_indexer([str(folder), "--db", str(db_path)], cwd=work)
        if proc.returncode != 0:
            raise RuntimeError(f"Sync failed: {proc.stderr}")
        rows = query_db(db_path)
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
        print("Test1 passed")

        # ---------- Test 2: remove one file, ensure DB updates ----------
        clean()
        (folder / "b.txt").unlink()
        proc = run_indexer([str(folder), "--db", str(db_path)], cwd=work)
        rows = query_db(db_path)
        assert len(rows) == 1 and rows[0][0] == b"a.txt", "File removal not reflected"
        print("Test2 passed")

        # ---------- Test 3: re-run, ensure no unnecessary MD5 recomputation ----------
        proc = run_indexer([str(folder), "--db", str(db_path)], cwd=work)
        # The MD5 should stay the same; we also verify that no MD5 computation
        # messages were printed (i.e., the script did not recompute hashes).
        assert proc.returncode == 0, "Re‑run failed"
        assert "Computed MD5" not in proc.stdout, "Unexpected MD5 recomputation"
        print("Test3 passed")

        # ---------- Test 4: modify the remaining file, MD5 should update ----------
        old_md5 = query_db(db_path)[0][1]
        (folder / "a.txt").write_text("hello modified")
        proc = run_indexer([str(folder), "--db", str(db_path)], cwd=work)
        new_md5 = query_db(db_path)[0][1]
        assert old_md5 != new_md5, "MD5 was not updated after modification"
        # Verify that MD5 recomputation was performed (message printed)
        assert "Computed MD5" in proc.stdout, "MD5 recomputation not reported"
        print("Test4 passed")

        # ---------- Test 5: duplicate file in second folder, -l 2 no report ----------
        # Keep the existing DB (from Test4) but clear the previous report.
        if report_path.exists():
            report_path.unlink()
        folder2 = work / "folder2"
        folder2.mkdir()
        shutil.copy2(folder / "a.txt", folder2 / "a.txt")
        proc = run_indexer([
            "-l", "2",
            str(folder), str(folder2),
            "--db", str(db_path),
            "--report", str(report_path),
        ], cwd=work)
        report = read_report(report_path)
        # With both copies identical, the limit check should produce no entries.
        assert "=== MISMATCH" not in report and "=== MISSING" not in report, "Unexpected entries in limit report"
        print("Test5 passed")

        # ---------- Test 6: validation report only MATCHes ----------
        # Use the existing DB (populated from previous tests) without cleaning.
        proc = run_indexer(["-v", "all", "--db", str(db_path), "--report", str(report_path)], cwd=work)
        report = read_report(report_path)
        # The report must contain a MATCH section, exactly two MATCH entries (one per folder),
        # and no MISMATCH/MISSING/NEW sections.
        assert "=== MATCH ===" in report, "MATCH section missing"
        match_lines = [line for line in report.splitlines() if line.startswith("MATCH:")]
        assert len(match_lines) == 2, f"Expected 2 MATCH entries, got {len(match_lines)}"
        # Ensure both folder names appear in the MATCH lines
        assert "folder" in report and "folder2" in report, "Both folders should be reported"
        assert "=== MISMATCH ===" not in report
        assert "=== MISSING ===" not in report
        assert "=== NEW ===" not in report
        print("Test6 passed")

        # ---------- Test 7: modify copy in second folder, limit report shows mismatch ----------
        clean()
        # Modify the copy in folder2
        (folder2 / "a.txt").write_text("different content")
        proc = run_indexer(["-l", "2", str(folder), str(folder2), "--db", str(db_path), "--report", str(report_path)], cwd=work)
        report = read_report(report_path)
        # Now there should be a line under MISMATCH (or at least a missing copy count < 2)
        # The limit check writes lines only for files with copies < limit.
        # Since folder2 file differs, the (full_path)#@#copies line should appear.
        assert "#@#" in report, "Limit report did not flag the mismatched copy"
        print("Test7 passed")

        print("All tests passed successfully.")


if __name__ == "__main__":
    main()
