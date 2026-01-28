# Notes: WSL Performance (Theory)

Below are theoretical ideas that could make WSL scans faster. No changes have been made to the code.

- Run the scanner inside WSL (Linux build). Accessing Linux files via `\\wsl$` from Windows is slower than running inside WSL and writing output to the Linux filesystem.
- Avoid per-file memory maps for tiny files. Use buffered reads for small files and reserve mmap for large files only.
- Batch output writes. Buffer matched paths per thread and flush in larger chunks to reduce lock contention on a single output file.
- Use a pipeline model. Separate walker threads from reader threads to keep file IO busy while parse work happens.
- Skip obvious binary/huge files sooner. Extension filters, size thresholds, or quick sampling can reduce total IO.
- Lower syscall count. Prefer APIs that reduce metadata+open calls or reuse handles where possible.
- OS-level indexing. Not available for WSL files, but would avoid full raw scans in other contexts.
