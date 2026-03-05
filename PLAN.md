# Plan: File Scanner Script

## Context
The user wants a Python script that scans a given folder, tracks files in a SQLite database, and provides several command‑line options for parallel MD5 computation, duplicate‑copy limits, and validation reporting. The database should live in the script's current directory (i.e., the directory where the script is executed). All reports (`report.log`) are written to the current working directory.

## Requirements (derived from AGENTS.md and user answers)
1. **Core functionality** – Recursively walk a top‑folder, collect file metadata, and keep a `files` table in SQLite with columns:
   - `filename` (basename)
   - `full_path` (absolute path)
   - `top_folder` (the top‑folder argument's basename)
   - `mtime` (last modification timestamp)
   - `md5` (hex digest of file contents)
   - `filesize` (size in bytes)
2. **Sync logic** –
   - Insert rows for files not present in the DB.
   - Delete rows for files that no longer exist.
   - Update rows where `mtime` or `filesize` changed (re‑compute MD5).
3. **Parallel MD5** – Use all available CPU cores by default; user can limit cores with `-n/--ncores`.
4. **`-l/--limit <N>`** – After the scan, ensure each unique `(full_path, md5)` appears in at least **N** distinct `top_folder`s. Write a `report.log` where each failing line is:
   ```
   <full_path>#@#<existing_copy_count>
   ```
5. **`-v/--validate [TOP]`** – Validate DB rows against the filesystem and generate a `report.log` containing:
   - Files that match their stored MD5.
   - Files that mismatch.
   - Missing files (present in DB, absent on disk).
   - New files (present on disk, absent in DB).
   The optional argument restricts validation to a specific `top_folder`; the value `all` validates every row.
6. **CLI** – Use `argparse` with the following options:
   - positional `top_folder`
   - `-n/--ncores`
   - `-l/--limit <N>`
   - `-v/--validate [TOP]`
   - `--db PATH` (optional, defaults to `files.db` in the current folder)
   - `--report PATH` (optional for future extensibility, default is `report.log` in cwd).

## Design Overview
1. **Entry point (`main`)** parses arguments and determines the operation mode.
2. **Database handling** – Helper class `FileDB` encapsulates connection, table creation, and CRUD helpers.
3. **Filesystem scan** – Function `scan_folder(top_folder)` yields a list of dicts with the required metadata (excluding MD5).
4. **MD5 computation** – Function `compute_md5(paths, ncores)` uses `multiprocessing.Pool` to hash files in parallel and returns a dict `{path: md5}`.
5. **Sync algorithm** –
   - Load existing DB rows into a dict keyed by `full_path`.
   - Walk the scan results, compare with DB dict.
   - Build three collections: `to_insert`, `to_update`, `to_delete`.
   - For `to_insert` and `to_update` compute MD5 via the parallel helper.
   - Execute DB inserts incrementally, as the MD5s are computed
6. **Limit check** – After sync, run a SQL query:
   ```sql
   SELECT full_path, md5, COUNT(DISTINCT top_folder) AS copies
   FROM files
   GROUP BY full_path, md5
   HAVING copies < :limit;
   ```
   Write each result line to `report.log` using the required `#@#` separator.
7. **Validation** – Load the rows to be validated (either all or filtered by `top_folder`). Compute MD5 for each existing file (parallel). Compare with stored MD5 and write a detailed `report.log` with sections `MATCH`, `MISMATCH`, `MISSING`, `NEW`.

## Modules / Functions
- `parse_args()` – returns `Namespace`.
- `FileDB(path)` – `__init__`, `ensure_table()`, `load_all()`, `insert_rows(rows)`, `update_rows(rows)`, `delete_paths(paths)`, `query_limit(limit)`.
- `scan_folder(top_folder)` – `os.walk`, collects `filename`, `full_path`, `top_folder`, `mtime`, `filesize`.
- `md5_worker(path)` – opens file in binary mode, reads in chunks (e.g., 4 MiB), returns hex digest.
- `compute_md5(paths, ncores)` – uses `Pool.map` with `md5_worker`.
- `perform_sync(db, top_folder, ncores)` – orchestrates steps 3‑5.
- `run_limit_check(db, limit, report_path)` – writes report.
- `run_validation(db, target, report_path, ncores)` – writes report.

## Parallelism Details
- Determine default cores with `os.cpu_count()`.
- If `ncores` is provided and >0, pass that as `processes` to `Pool`.
- MD5 worker reads file in 4 MiB chunks to avoid loading whole files into memory.
- For large directories, the list of paths for MD5 is split among workers automatically by `Pool.map`.

## Testing / Verification
- Create a test directory "data" with 10 small files to use for testing
- Run script on the test directory "data" and verify DB populated.
- Add a file foobar inside "data" , rerun, verify DB gets populated with new file
- Modify file foobar, re‑run, ensure row gets updated.
- Delete file foobar, re‑run, ensure row removed.
- Copy folder "data" to "data2", re-run for "data2", verify DB gets populated for that folder too.
- Remove one file from data2, and use `-l 2` to re-run; verify `report.log` contains the one and only one file that only exists in "data".
- Use `-v` and validate the `report.log` output.

## Files to be Created / Modified
- New script file (`indexer.py`) placed in the current working directory.
- No existing project files need changes.

## Open Questions (none after user clarification)
All required decisions have been answered by the user.

---
*Implementation will follow this plan exactly.*
