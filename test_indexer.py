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


# Deterministic config baseline for every child run. load_config() honours
# $INDEXER_CONFIG *ahead* of the "indexer.toml beside the script" lookup, and
# /dev/null is not a regular file - so a default run always reports
# "no config file: excluding nothing" no matter what (gitignored) indexer.toml
# happens to sit next to indexer.py on the developer's box. Pass config= to
# opt into a real one.
CONFIG_BASELINE = os.devnull


def run_indexer(args, cwd=None, env=None, config=CONFIG_BASELINE):
    """Run ``indexer.py`` with the given *args* and return the completed process.

    ``cwd`` defaults to the current working directory; a temporary directory is
    used for isolation in the test suite.

    ``config`` is the file the child is told to read via ``$INDEXER_CONFIG``;
    the default is the unusable ``CONFIG_BASELINE`` path, i.e. "no config at
    all", which keeps the exclusions asserted below reproducible. ``env``, when
    given, replaces the child environment wholesale.
    """
    if env is None:
        env = os.environ.copy()
        env["INDEXER_CONFIG"] = config
    return subprocess.run(
        [sys.executable, str(INDEXER)] + args,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        env=env,
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
        assert "Skipping missing (or non-folder)" in proc.stdout
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
        ok = sum(1 for _, m, _ in results if m == "deadbeef")
        none = sum(1 for _, m, _ in results if m is None)
        degraded = sum(1 for _, m, d in results if m is None and d)
        assert ok == batch and none == n - batch, \
            f"expected {batch} hashed + {n-batch} None, got {ok} + {none}"
        assert degraded == n - batch, \
            f"pool-death Nones must be marked degraded, got {degraded}"
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

        # Test 21: indexer.toml drives BOTH kinds of exclusion. A directory
        # whose path holds a drop_dir_token vanishes with its whole subtree,
        # and a file whose NAME holds a drop_name_substrings entry is skipped
        # while its siblings stay.
        clean()
        cfg21 = work / "cfg21.toml"
        cfg21.write_text('drop_dir_tokens = ["exclude_me"]\n'
                         'drop_name_substrings = [".skip_"]\n',
                         encoding="utf-8")
        c21 = work / "cfg21"
        c21.mkdir()
        (c21 / "keep.txt").write_text("keep")
        (c21 / "noise.skip_.txt").write_text("skip me")
        hidden = c21 / "exclude_me"
        hidden.mkdir()
        (hidden / "underneath.txt").write_text("should never be seen")
        proc = run_indexer([str(c21), "--db", str(db_path)], cwd=work,
                           config=str(cfg21))
        assert proc.returncode == 0, proc.stderr
        paths = {r[0] for r in query_db(db_path)}
        assert paths == {b"keep.txt"}, f"config exclusions wrong: {paths}"
        # The config in force, and its effective size, must be on the status
        # line - an ignored config file must never look like a success.
        assert str(cfg21) in proc.stdout, "config path not reported"
        assert "1 dir tokens, 1 name skips" in proc.stdout, proc.stdout
        print("Test21 passed")

        # Test 22: db/report come from the config when the command line is
        # silent, and the command line still overrides both when explicit.
        clean()
        cfg22 = work / "cfg22.toml"
        db_cfg = work / "fromcfg.db"
        rep_cfg = work / "fromcfg.log"
        cfg22.write_text(f'db = "{db_cfg}"\nreport = "{rep_cfg}"\n',
                         encoding="utf-8")
        c22 = work / "cfg22"
        c22.mkdir()
        (c22 / "a.txt").write_text("aaa")
        proc = run_indexer([str(c22)], cwd=work, config=str(cfg22))
        assert proc.returncode == 0, proc.stderr
        assert db_cfg.exists(), "config db= was not honoured"
        assert not db_path.exists(), "default files.db used despite config db="
        assert f"db={db_cfg}" in proc.stdout, proc.stdout
        proc = run_indexer(["-v", "all", str(c22)], cwd=work,
                           config=str(cfg22))
        assert proc.returncode == 0, proc.stderr
        assert rep_cfg.exists(), "config report= was not honoured"
        # Now override both on the command line.
        clean()
        rep_cfg.unlink(missing_ok=True)
        proc = run_indexer([str(c22), "--db", str(db_path),
                            "--report", str(report_path)], cwd=work,
                           config=str(cfg22))
        assert proc.returncode == 0, proc.stderr
        assert db_path.exists(), "--db override failed"
        assert f"db={db_path}" in proc.stdout, proc.stdout
        proc = run_indexer(["-v", "all", str(c22), "--db", str(db_path),
                            "--report", str(report_path)], cwd=work,
                           config=str(cfg22))
        assert report_path.exists(), "--report override failed"
        assert not rep_cfg.exists(), "config report= used over --report"
        print("Test22 passed")

        # Test 23: a config key of the wrong type aborts the run cleanly,
        # before anything is indexed - never a traceback, never a partial DB.
        clean()
        bad_list = work / "badlist.toml"
        bad_list.write_text('drop_dir_tokens = "exclude_me"\n',
                            encoding="utf-8")
        c23 = work / "cfg23"
        c23.mkdir()
        (c23 / "a.txt").write_text("aaa")
        proc = run_indexer([str(c23), "--db", str(db_path)], cwd=work,
                           config=str(bad_list))
        assert proc.returncode == 1, "bad list type should abort the run"
        assert "must be a list of strings" in proc.stdout, proc.stdout
        assert "Traceback" not in proc.stderr, proc.stderr
        assert not db_path.exists(), "DB was created despite the bad config"
        bad_str = work / "badstr.toml"
        bad_str.write_text("db = 5\n", encoding="utf-8")
        proc = run_indexer([str(c23), "--db", str(db_path)], cwd=work,
                           config=str(bad_str))
        assert proc.returncode == 1, "bad db type should abort the run"
        assert "must be a str" in proc.stdout, proc.stdout
        assert "Traceback" not in proc.stderr, proc.stderr
        print("Test23 passed")

        # Test 24: an unknown key is warned about and ignored, and the run
        # proceeds. This is the misspelled-key case (note the typo below).
        clean()
        cfg24 = work / "unk.toml"
        cfg24.write_text('drop_dir_tokens = ["exclude_me"]\n'
                         'drop_dire_tokens = ["typoed"]\n', encoding="utf-8")
        c24 = work / "cfg24"
        c24.mkdir()
        (c24 / "a.txt").write_text("aaa")
        proc = run_indexer([str(c24), "--db", str(db_path)], cwd=work,
                           config=str(cfg24))
        assert proc.returncode == 0, proc.stderr
        assert "ignoring unknown key" in proc.stdout, proc.stdout
        assert "drop_dire_tokens" in proc.stdout, "typoed key not named"
        assert len(query_db(db_path)) == 1, "unknown key changed behaviour"
        print("Test24 passed")

        # Test 25: no config file at all. The run must go ahead and index
        # EVERYTHING, and say so explicitly - "carries no policy of its own"
        # has to be visible in the log, not silently assumed.
        clean()
        proc = run_indexer([str(c21), "--db", str(db_path)], cwd=work,
                           config=str(work / "absent.toml"))
        assert proc.returncode == 0, proc.stderr
        assert "no config file: excluding nothing" in proc.stdout, proc.stdout
        assert "0 dir tokens, 0 name skips" in proc.stdout, proc.stdout
        paths = {r[0] for r in query_db(db_path)}
        assert b"noise.skip_.txt" in paths and b"exclude_me/underneath.txt" \
            in paths, f"nothing should have been excluded: {paths}"
        print("Test25 passed")

        # Test 26: _evict_pages must fdatasync FIRST and only then advise the
        # kernel to drop the pages - dirty pages cannot be evicted, so without
        # the sync a freshly-written file would still be summed out of its own
        # page cache and the MD5 would stop covering the medium. Both calls are
        # best-effort: neither may fail the run.
        import indexer as _ix
        calls = []
        real_sync, real_advise = _ix.os.fdatasync, _ix.os.posix_fadvise
        try:
            _ix.os.fdatasync = lambda fd: calls.append(("fdatasync", fd))
            _ix.os.posix_fadvise = lambda fd, off, length, adj: \
                calls.append(("posix_fadvise", fd, adj))
            _ix._evict_pages(7)
            assert [c[0] for c in calls] == ["fdatasync", "posix_fadvise"], \
                f"wrong order/coverage: {calls}"
            assert calls[0][1] == 7 and calls[1][1] == 7, "fd not passed to both"
            assert calls[1][2] == os.POSIX_FADV_DONTNEED, \
                "must advise POSIX_FADV_DONTNEED, got " + repr(calls[1][2])

            def _boom(*_args, **_kw):
                raise OSError("simulated EIO")

            calls.clear()
            _ix.os.fdatasync = _boom
            _ix.os.posix_fadvise = _boom
            _ix._evict_pages(7)          # must swallow, not propagate
            print("Test26 passed")
        finally:
            _ix.os.fdatasync = real_sync
            _ix.os.posix_fadvise = real_advise

        # Test 27: exclusions are matched on BYTES, so they must keep working
        # for names that are not valid UTF-8 (surrogateescape paths) and for
        # non-ASCII tokens. A str/bytes mix-up here silently excludes nothing.
        clean()
        cfg27 = work / "bytes.toml"
        cfg27.write_text('drop_name_substrings = ["weird", "\u03b1\u03b2"]\n',
                         encoding="utf-8")
        c27 = work / "cfg27"
        c27.mkdir()
        (c27 / "keep.dat").write_text("keep")
        with open(os.path.join(str(c27).encode(), b"weird\xff.dat"), "wb") as fh:
            fh.write(b"undecodable name\n")
        (c27 / "\u03b1\u03b2\u03b3_skipme.dat").write_text("greek")
        proc = run_indexer([str(c27), "--db", str(db_path)], cwd=work,
                           config=str(cfg27))
        assert proc.returncode == 0, proc.stderr
        paths = {r[0] for r in query_db(db_path)}
        assert paths == {b"keep.dat"}, f"byte-path exclusions wrong: {paths}"
        assert "2 name skips" in proc.stdout, proc.stdout
        print("Test27 passed")

        # Test 28: an indexer.toml that cannot be parsed (or read) must abort
        # the run with a clear message. Falling back to "exclude nothing" here
        # would quietly index a box whose policy is precisely to exclude
        # something, so a garbled config has to be fatal, not ignorable.
        clean()
        broken = work / "broken.toml"
        broken.write_text('drop_dir_tokens = ["unterminated\n',
                          encoding="utf-8")
        proc = run_indexer([str(c21), "--db", str(db_path)], cwd=work,
                           config=str(broken))
        assert proc.returncode == 1, "unparsable config should abort the run"
        assert "Error: cannot read" in proc.stdout, proc.stdout
        assert "Traceback" not in proc.stderr, proc.stderr
        assert not db_path.exists(), "DB was created despite the bad config"
        if os.geteuid() != 0:
            clean()
            locked = work / "locked.toml"
            locked.write_text('drop_dir_tokens = ["x"]\n', encoding="utf-8")
            os.chmod(locked, 0)
            try:
                proc = run_indexer([str(c21), "--db", str(db_path)], cwd=work,
                                   config=str(locked))
                assert proc.returncode == 1, "unreadable config should abort"
                assert "Error: cannot read" in proc.stdout, proc.stdout
                assert not db_path.exists()
            finally:
                os.chmod(locked, 0o644)
            print("Test28 passed")
        else:
            print("Test28 partially skipped (running as root: "
                  "unreadable-file half needs non-root)")

    print("All tests passed successfully.")


if __name__ == "__main__":
    main()
