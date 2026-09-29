# FileIndexer - Cross-drive file redundancy & integrity checking.

[![CI](https://github.com/ttsiodras/FileIndexer/actions/workflows/ci.yml/badge.svg)](https://github.com/ttsiodras/FileIndexer/actions/workflows/ci.yml)

*([Blog post](https://www.thanassis.space/indexer.html): easier-to read,
including the backstory of this utility)*

Tracks files across mounted folders in a single SQLite database, doing MD5 csums
in parallel, and verifies redundancy and integrity — for example across several
external/mounted USB drives that are supposed to contain copies of the same data.

The tool is a single Python script (`indexer.py`) with **no runtime
dependencies** — it uses only the standard library.

## Quick start

```sh
python3 indexer.py -n 1 /mnt/usb1 /mnt/usb2   # index your drives, using one core only
                                              # (avoid thrashing mechanical drives)
python3 indexer.py /mnt/usb3ssd           # index external SSD, using all cores available
python3 indexer.py -l 2                   # which files exist on fewer than 2 drives?
python3 indexer.py -v                     # re-hash the drives and verify every checksum
```

## What it does

- **Sync** — recursively indexes one or more folders into a SQLite database.
  New files are inserted, changed files (different size or mtime) are re-hashed,
  and rows for files that no longer exist are removed. Note that sync detects
  changes by size and mtime, just as rsync does.
- **Limit check** (`-l/--limit`) — flags files that appear in fewer than *N*
  distinct top folders, so you can spot copies that are missing from some of
  your drives.
- **Validate** (`-v/--validate`) — re-computes every MD5 on disk and compares it
  against the database, so you can detect silently corrupted or missing files.
- **Parallel hashing** — MD5 computation is spread across all cores by default;
  tune it with `-n/--ncores`. On spinning-disk (non-SSD) drives, multiple
  concurrent readers can be much slower than a single reader and increase
  wear — prefer `-n 1` there.

## Database

By default everything is stored in `files.db` in the current folder.
Each row of table `files` holds:

| column      | meaning                                    |
|-------------|--------------------------------------------|
| `filename`  | bare file name                             |
| `full_path` | path relative to the scanned `top_folder`  |
| `top_folder`| the root folder that was scanned           |
| `mtime`     | modification time used to detect changes   |
| `md5`       | checksum                                   |
| `filesize`  | size in bytes                              |

Paths are stored as raw bytes so that even non-UTF-8 file names are handled safely.

## Usage

```
python3 indexer.py [options] [FOLDER ...]

Folders are required for sync, but optional for `-l` (no folders = check all
folders in the database) and for `-v` (which can take its own target via
`-v [FOLDER]` or `-v all` to verify all drives).
```

### Sync folders into the index

```sh
# Index two external non-SSD USB drives
python3 indexer.py -n 1 /mnt/usb1 /mnt/usb2

# Use a specific database and limit parallel hashing to 4 cores
python3 indexer.py -n 4 --db /data/files.db /mnt/ssd
```

### Find files that lack redundancy

```sh
# Report every full_path that exists in fewer than 2 distinct top folders
python3 indexer.py -l 2
```

The `-l` mode writes a low-redundancy report to `report.log` (override with
`--report`). Passing a list of folders scopes the check to exactly those folders;
with no folders passed, it checks every folder currently in the database
(so the `indexer -l 2` example above will tell you which files are stored in
only one drive).

### Validate the database against the filesystem

```sh
# Validate every stored row
python3 indexer.py -v

# Validate only a single folder
python3 indexer.py -v /mnt/usb1
```

The results are written to `report.log` (override with `--report`). Each file is
classified as `MATCH` (on disk with the expected MD5), `MISMATCH` (on disk but
MD5 differs, or unreadable), `MISSING` (in the DB but not on disk) or `NEW`
(on disk but not in the DB); problems are also echoed to the console. A sample
`report.log` looks like:

    === MISMATCH ===
    MISMATCH: /mnt/usb1/backup.tgz (expected=abc..., actual=def...)
    === MISSING ===
    MISSING: /mnt/usb2/docs/report.pdf (expected_md5=0123...)
    === NEW ===
    NEW: /mnt/usb1/temp/scratch.bin

Validation and limit-check are mutually exclusive (`--validate` and `--limit`
can't be combined).

### Options

```
positional:
  top_folder             folder(s) to scan

optional:
  -n, --ncores N         parallel workers for MD5 hashing (default: all cores)
  -l, --limit N          flag files present in fewer than N top_folders
  -v, --validate [T]     validate DB against filesystem (T = a folder or
                         'all'; default: 'all')
  --db PATH              SQLite database path (default: from indexer.toml,
                         else files.db)
  --report PATH          report path (default: from indexer.toml,
                         else report.log)
  --config PATH          config file to use instead of the indexer.toml next
                         to this script (also honours $INDEXER_CONFIG)
  -h, --help             show help
```

### Per-machine configuration (indexer.toml)

The script itself carries **no exclusion policy**: a given box's exclusions are
meaningless (and sometimes confidential) on another box. Per-machine settings
live in an `indexer.toml` beside `indexer.py`, which is gitignored; copy
`indexer.toml.example` to get started.

```toml
drop_dir_tokens      = ["ai_model_weights", "scratch"]   # skip dir + subtree
drop_name_substrings = ["preview_", ".deprecated"]       # skip files by name
db     = "files.db"      # default for --db
report = "report.log"    # default for --report
```

Lookup order is `--config`, then `$INDEXER_CONFIG`, then `indexer.toml` beside
the resolved script path. Command line options always override the file.

A missing file is neither an error nor a warning: the run goes ahead indexing
**everything**, and the status line reports that explicitly as
`no config file: excluding nothing ... 0 dir tokens`, so a silently-ignored
config never looks like a successful exclusion. Unknown keys are warned about
and ignored; a key of the wrong type aborts the run before any indexing.

## Requirements

- **Python 3.11+** — the script needs no third-party packages; the standard
  library is enough. `3.11` is the floor because `tomllib` (used to read
  `indexer.toml`) entered the stdlib there.
- For development (linting, type-checking, coverage etc) the Makefile will install
  a set of tools from `requirements-dev.txt`.

## Development

Install the development/quality tooling into a virtualenv and run the standard
checks with `make`:

```sh
make            # set up venv, then run flake8 + pylint + mypy
make test       # run the end-to-end test suite
make coverage   # run tests under coverage and report indexer.py coverage
make clean      # remove build artifacts and the virtualenv marker
```

Individual checks can be run with `make flake8`, `make pylint`, `make mypy`.

The test suite (`test_indexer.py`) drives the tool end-to-end by launching it as
a subprocess against temporary folders.

## Layout

```
indexer.py           the tool (single file, stdlib only)
indexer.toml.example template for the per-machine config (indexer.toml is gitignored)
test_indexer.py      end-to-end test suite
Makefile             developer tasks
pylint.cfg           lint configuration
requirements.txt     runtime deps (intentionally empty)
requirements-dev.txt dev/quality tooling
AI.prompts/          the prompts used to develop the tool
LICENSE              MIT license
```

## License

See file LICENSE.
