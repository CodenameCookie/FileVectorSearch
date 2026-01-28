# FileVectorSearch

Fast file content + filename searcher for a whole drive or folder. It scans file contents for a regex pattern (default: `FileVector\\w*`), and can optionally match filenames/paths too.

## Build

```powershell
cargo build --release
```

## Run (recommended)

```powershell
.\target\release\file_vector_search.exe --root A:\home --out filevector_files.txt --match-names
```

## Full Flags

- `--root PATH` : Root folder/drive to scan. Default: `A:\`
- `--out FILE` : Output file with matched paths. Default: `filevector_files.txt`
- `--pattern REGEX` : Regex pattern (Rust regex syntax). Default: `FileVector\\w*`
- `--case-sensitive` : Use case-sensitive matching. Default: case-insensitive
- `--threads N` : Worker threads for the directory walk. Default: auto
- `--no-count` : Skip the initial count pass (starts scanning immediately, no percent)
- `--match-names` : Also match on file paths/names (not just file contents)
- `--verbose` : Print the first non-permission error encountered
- `--log-permission-denied` : Allow permission-denied errors to be printed
- `--stop-on-first-error` : Stop after printing the first error
- `--flush-every N` : Flush matched output after N paths per thread. Default: 500
- `--cache PATH` : SQLite cache path for metadata. Disabled unless set.
- `--cache-mode off|read|write|readwrite` : Cache behavior. Default: off.
- `--cache-reset` : Reset cache schema and data.
- `--cache-unlock` : Unlock-only mode; ignores other flags and exits after attempting to open known cache paths.
- `--cache-kill` : Kill other running `file_vector_search` processes and exit.
- `--cache-local` : Use local Windows cache path (LOCALAPPDATA\\file_vector_search\\cache.sqlite). PowerShell only.

## Examples

Scan a Windows users folder:

```powershell
.\target\release\file_vector_search.exe --root C:\Users --out filevector_files.txt --match-names
```

Case-sensitive match on filenames and contents:

```powershell
.\target\release\file_vector_search.exe --root A:\home --out filevector_files.txt --match-names --case-sensitive
```

Start immediately without counting:

```powershell
.\target\release\file_vector_search.exe --root A:\ --out filevector_files.txt --no-count
```

Use local Windows cache to avoid WSL lock issues:

```powershell
.\target\release\file_vector_search.exe --root \\wsl$\Ubuntu-24.04\home --out filevector_files.txt --match-names --cache-local --cache-mode readwrite
```

Terminate other running searchers (if a cache is locked):

```powershell
.\target\release\file_vector_search.exe --cache-kill
```

Probe a cache lock (does not force-unlock):

```powershell
.\target\release\file_vector_search.exe --cache-unlock
```

## Notes

- Permission-denied (OS error 5) means the OS blocked access to a file or folder. The tool skips those by default.
- Progress updates print every 1,000 scanned files.
- `--cache-unlock` is a probe; SQLite cannot force-unlock a file. If it reports locked, another process or filesystem locking is still holding it.

## Future Improvements

- Add a cache metrics summary (cached hits vs scanned) for clearer reporting.
- Prefer buffered reads for small files and mmap for large files only.
- Add a pipeline model (separate walker/read/regex stages) to keep IO busy.
- Add quick binary/filetype filtering to reduce unnecessary reads.
- Add a --skip-dirs flag to exclude common library/build folders (node_modules, dist, target, etc.).
- Improve WSL behavior by running the scanner inside WSL when possible.

