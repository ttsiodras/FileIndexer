#!/usr/bin/env python3
"""Test suite for ``indexer.py`` based on the steps described in ``TEST.md``.

The script creates temporary directories, runs the indexer with the appropriate
options and asserts the expected state of the SQLite database and the generated
``report.log``.
"""

import os
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
        assert "computed MD5" not in proc.stdout, "Unexpected MD5 recomputation"
        print("Test3 passed")

        # ---------- Test 4: modify the remaining file, MD5 should update ----------
        old_md5 = query_db(db_path)[0][1]
        (folder / "a.txt").write_text("hello modified")
        proc = run_indexer([str(folder), "--db", str(db_path)], cwd=work)
        new_md5 = query_db(db_path)[0][1]
        assert old_md5 != new_md5, "MD5 was not updated after modification"
        # Verify that MD5 recomputation was performed (message printed)
        assert "computed MD5" in proc.stdout, "MD5 recomputation not reported"
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
        # With both copies identical and limit 2, the report must be empty:
        # every (full_path, md5) appears in >= 2 top_folders.
        assert report.strip() == "", "Limit report should be empty"
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

    # ---------- Additional tests: error paths and edge cases (raise coverage) ----------
    with tempfile.TemporaryDirectory() as tmpdir2:
        work = Path(tmpdir2)
        db_path = work / "test.db"
        report_path = work / "report.log"

        def clean():
            for p in (db_path, report_path):
                if p.exists():
                    p.unlink()
            for suf in ("-wal", "-shm", "-journal"):
                q = Path(str(db_path) + suf)
                if q.exists():
                    q.unlink()

        def sync_folder(folder, expected_count=None):
            proc = run_indexer([str(folder), "--db", str(db_path)], cwd=work)
            assert proc.returncode == 0, proc.stderr
            if expected_count is not None:
                assert len(query_db(db_path)) == expected_count, \
                    f"Expected {expected_count} rows"

        # Test 8: a deleted file is removed from the DB and reported
        clean()
        d = work / "dele"
        d.mkdir()
        (d / "keep.txt").write_text("keep me")
        sync_folder(d, 1)
        (d / "keep.txt").unlink()
        proc = run_indexer([str(d), "--db", str(db_path)], cwd=work)
        assert proc.returncode == 0
        assert len(query_db(db_path)) == 0, "Deleted file still in DB"
        assert "Deleted (missing)" in proc.stdout
        print("Test8 passed")

        # Test 9: validation flags MATCH, MISMATCH, MISSING and NEW together
        clean()
        f = work / "val"
        f.mkdir()
        for n, content in {"a.txt": "alpha", "b.txt": "beta",
                           "c.txt": "gamma"}.items():
            (f / n).write_text(content)
        sync_folder(f, 3)
        (f / "a.txt").write_text("ALPHA-CHANGED")   # content changed -> MISMATCH
        (f / "b.txt").unlink()                       # removed        -> MISSING
        (f / "d.txt").write_text("delta brand new")  # new            -> NEW
        proc = run_indexer(["-v", "all", "--db", str(db_path),
                            "--report", str(report_path)], cwd=work)
        assert proc.returncode == 0
        report = read_report(report_path)
        assert "=== MATCH ===" in report
        assert "=== MISMATCH ===" in report
        assert "=== MISSING ===" in report
        assert "=== NEW ===" in report
        assert any(line.startswith("MISMATCH:") and "a.txt" in line
                   for line in report.splitlines())
        assert any(line.startswith("MISSING:") and "b.txt" in line
                   for line in report.splitlines())
        assert any(line.startswith("NEW:") and "d.txt" in line
                   for line in report.splitlines())
        print("Test9 passed")

        # Test 10: validation restricted to a single folder (-v FOLDER)
        clean()
        g = work / "single"
        g.mkdir()
        (g / "s.txt").write_text("solo")
        sync_folder(g, 1)
        proc = run_indexer(["-v", str(g), "--db", str(db_path),
                            "--report", str(report_path)], cwd=work)
        assert proc.returncode == 0
        assert "=== MATCH ===" in read_report(report_path)
        print("Test10 passed")

        # Test 11: --validate and --limit are mutually exclusive
        clean()
        proc = run_indexer(["-v", "all", "-l", "2", str(g),
                            "--db", str(db_path)], cwd=work)
        assert proc.returncode != 0
        assert "mutually exclusive" in proc.stderr
        print("Test11 passed")

        # Test 12: scanning a nonexistent folder gives a clean non-zero exit
        # (warn + skip, no traceback).
        clean()
        proc = run_indexer([str(work / "does_not_exist"),
                            "--db", str(db_path)], cwd=work)
        assert proc.returncode != 0
        assert "Skipping missing folder" in proc.stdout
        assert "Traceback" not in proc.stderr
        print("Test12 passed")

        # Test 13: running with no arguments prints help and exits
        proc = run_indexer([], cwd=work)
        assert proc.returncode == 1
        assert "usage:" in proc.stdout
        print("Test13 passed")

        # Test 14: symbolic links are skipped
        clean()
        sl = work / "links"
        sl.mkdir()
        (sl / "real.txt").write_text("real content")
        os.symlink("real.txt", sl / "link.txt")
        sync_folder(sl, 1)
        paths = [row[0] for row in query_db(db_path)]
        assert b"real.txt" in paths
        assert b"link.txt" not in paths, "Symlink was indexed"
        print("Test14 passed")

        # Test 15: -v all skips a top_folder that no longer exists
        clean()
        gone = work / "gone"
        gone.mkdir()
        (gone / "a.txt").write_text("x")
        sync_folder(gone, 1)
        shutil.rmtree(gone)
        proc = run_indexer(["-v", "all", "--db", str(db_path),
                            "--report", str(report_path)], cwd=work)
        assert proc.returncode == 0
        assert "Top folder missing, skipping" in proc.stdout
        print("Test15 passed")

        # Test 16: unreadable file -> MD5 error (skipped when running as root)
        if os.geteuid() != 0:
            clean()
            u = work / "unread"
            u.mkdir()
            (u / "secret.txt").write_text("sensitive data")
            os.chmod(u / "secret.txt", 0)
            try:
                proc = run_indexer([str(u), "--db", str(db_path)], cwd=work)
                assert "MD5 ERROR" in proc.stdout
                rows = query_db(db_path)
                assert any(r[0] == b"secret.txt" and r[1] is None
                           for r in rows)
            finally:
                os.chmod(u / "secret.txt", 0o644)
            print("Test16 passed")
        else:
            print("Test16 skipped (running as root)")

        # Test 17: many files exercise the bounded refill and the progress print
        clean()
        big = work / "big"
        big.mkdir()
        for i in range(1000):
            (big / f"f{i:04d}.txt").write_text(str(i))
        proc = run_indexer([str(big), "--db", str(db_path)], cwd=work)
        assert proc.returncode == 0
        assert len(query_db(db_path)) == 1000
        print("Test17 passed")

        # Test 18: an unreadable subdirectory does NOT delete the rows under it
        if os.geteuid() != 0:
            clean()
            up = work / "unreaddir"
            up.mkdir()
            inner = up / "sub"
            # A file nested 3 levels under the soon-to-be-unreadable dir.
            deep = inner / "level1" / "level2" / "level3"
            deep.mkdir(parents=True)
            (deep / "keep.txt").write_text("keep me")
            (up / "gone.txt").write_text("delete me")
            # First sync indexes both files.
            proc = run_indexer([str(up), "--db", str(db_path)], cwd=work)
            assert proc.returncode == 0, proc.stderr
            paths = {r[0] for r in query_db(db_path)}
            assert b"sub/level1/level2/level3/keep.txt" in paths \
                and b"gone.txt" in paths
            # Make the subdir unreadable AND truly delete gone.txt, then re-sync.
            os.chmod(inner, 0)
            try:
                (up / "gone.txt").unlink()
                proc = run_indexer([str(up), "--db", str(db_path)], cwd=work)
                assert "Unreadable directory, skipping" in proc.stdout, \
                    "No unreadable-dir warning printed"
                paths = {r[0] for r in query_db(db_path)}
                assert b"sub/level1/level2/level3/keep.txt" in paths, \
                    "Row 3 levels under unreadable dir was wrongly deleted"
                assert b"gone.txt" not in paths, \
                    "Genuinely deleted file was not removed"
            finally:
                os.chmod(inner, 0o755)
            print("Test18 passed")
        else:
            print("Test18 skipped (running as root)")

        # Test 19: a dead hashing pool must not abort the sync (refill guard).
        # Simulate a killed worker by making the pool's submit raise
        # BrokenProcessPool after the initial window is submitted; stream_md5s
        # must degrade the remaining files to md5=None instead of tracebacking.
        import concurrent.futures as _cf
        import indexer as _ix
        from concurrent.futures.process import BrokenProcessPool
        from concurrent.futures import Future

        class _DeadPool:
            def __init__(self, max_workers, fail_after):
                self.calls = 0
                self.fail_after = fail_after

            def __enter__(self):
                return self

            def __exit__(self, *a):
                self.shutdown()

            def shutdown(self, wait=True):
                pass

            def submit(self, fn, *args):
                self.calls += 1
                if self.calls > self.fail_after:
                    raise BrokenProcessPool("simulated worker death")
                fut = Future()
                fut.set_result("deadbeef")
                return fut

        n = 20
        batch = 8
        items = [
            _ix.FileMetadata(f"f{i}".encode(), f"f{i}".encode(),
                             b"/tmp/x", 0.0, 10)
            for i in range(n)
        ]
        orig_pool = _ix.ProcessPoolExecutor
        _ix.ProcessPoolExecutor = (
            lambda max_workers=1, **kw: _DeadPool(max_workers, batch))
        try:
            results = list(_ix.stream_md5s(items, 4, batch=batch))
        finally:
            _ix.ProcessPoolExecutor = orig_pool
        assert len(results) == n, \
            f"stream_md5s yielded {len(results)} of {n} after pool death"
        ok = sum(1 for _, m in results if m == "deadbeef")
        none = sum(1 for _, m in results if m is None)
        assert ok == batch and none == n - batch, \
            f"expected {batch} hashed + {n-batch} None, got {ok} + {none}"
        yielded = sorted(r[0].full_path for r in results)
        expected = sorted(it.full_path for it in items)
        assert yielded == expected, "items got lost/reordered on pool death"
        print("Test19 passed")

        # Test 20: a --db stored inside a scanned top_folder is rejected at
        # launch (fail fast, before any walk or DB write).
        clean()
        scandb = work / "scandb"
        scandb.mkdir()
        (scandb / "a.txt").write_text("hi")
        db_inside = scandb / "x.db"
        proc = run_indexer([str(scandb), "--db", str(db_inside)], cwd=work)
        assert proc.returncode == 1, "DB-inside-scan should fail immediately"
        assert "inside folder being scanned" in proc.stdout
        assert not db_inside.exists(), "DB was created despite the guard"
        print("Test20 passed")

    print("All tests passed successfully.")


if __name__ == "__main__":
    main()
