use ignore::{WalkBuilder, WalkState};
use memmap2::Mmap;
use regex::bytes::Regex;
use regex::Regex as TextRegex;
use rusqlite::ffi::ErrorCode;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheMode {
    Off,
    Read,
    Write,
    ReadWrite,
}

impl CacheMode {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "off" => Ok(Self::Off),
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "readwrite" => Ok(Self::ReadWrite),
            _ => Err("--cache-mode must be one of: off, read, write, readwrite".to_string()),
        }
    }

    fn allows_read(self) -> bool {
        matches!(self, Self::Read | Self::ReadWrite)
    }

    fn allows_write(self) -> bool {
        matches!(self, Self::Write | Self::ReadWrite)
    }
}

#[derive(Clone, Debug)]
struct CacheConfig {
    path: PathBuf,
    mode: CacheMode,
}

impl CacheConfig {
    fn open_thread_connection(&self) -> Option<Connection> {
        match Connection::open(&self.path) {
            Ok(conn) => Some(configure_cache_connection(conn)),
            Err(err) => {
                eprintln!("Cache open failed ({}): {}", self.path.display(), err);
                None
            }
        }
    }
}

fn now_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => dur.as_nanos().min(i64::MAX as u128) as i64,
        Err(_) => 0,
    }
}

fn modified_time_ns(meta: &std::fs::Metadata) -> Option<i64> {
    let modified = meta.modified().ok()?;
    let dur = modified.duration_since(UNIX_EPOCH).ok()?;
    Some(dur.as_nanos().min(i64::MAX as u128) as i64)
}

fn init_cache_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;

        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dirs (
            path TEXT PRIMARY KEY,
            mtime_ns INTEGER NOT NULL,
            last_scanned_ns INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            dir_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            last_scanned_ns INTEGER NOT NULL,
            last_match INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_files_dir_path ON files(dir_path);
        CREATE INDEX IF NOT EXISTS idx_dirs_mtime ON dirs(mtime_ns);
        ",
    )?;
    Ok(())
}

fn reset_cache(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS files;
        DROP TABLE IF EXISTS dirs;
        DROP TABLE IF EXISTS meta;
        ",
    )?;
    init_cache_schema(conn)
}

fn is_locked_error(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(code, _) => {
            code.code == ErrorCode::DatabaseBusy || code.code == ErrorCode::DatabaseLocked
        }
        _ => false,
    }
}

fn configure_cache_connection(conn: Connection) -> Connection {
    let _ = conn.busy_timeout(std::time::Duration::from_secs(5));
    conn
}

fn known_cache_paths() -> Vec<PathBuf> {
    let mut paths = vec![
        PathBuf::from(".filevector_cache.sqlite"),
        PathBuf::from(r"C:\\Users\\Work\\filevector_cache.sqlite"),
    ];
    if let Ok(local) = local_cache_path_string() {
        paths.push(PathBuf::from(local));
    }
    paths
}

fn try_unlock_cache(path: &Path) -> Result<(), String> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|err| format!("open failed: {}", err))?;
    let conn = configure_cache_connection(conn);

    conn.execute("PRAGMA user_version;", [])
        .map_err(|err| format!("{}", err))?;
    drop(conn);
    Ok(())
}

fn run_cache_unlock() -> i32 {
    let mut any_success = false;

    for path in known_cache_paths() {
        if !path.exists() {
            eprintln!("Cache unlock: {} (missing)", path.display());
            continue;
        }

        match try_unlock_cache(&path) {
            Ok(()) => {
                any_success = true;
                eprintln!("Cache unlock: {} (ok)", path.display());
            }
            Err(err) => {
                eprintln!("Cache unlock: {} ({})", path.display(), err);
            }
        }
    }

    if any_success {
        0
    } else {
        4
    }
}

fn cache_skip_dir(conn: &Connection, mode: CacheMode, path: &Path, meta: &std::fs::Metadata) -> bool {
    if !mode.allows_read() {
        return false;
    }

    let mtime_ns = match modified_time_ns(meta) {
        Some(v) => v,
        None => return false,
    };

    let path_s = path.to_string_lossy();
    let cached: Option<i64> = conn
        .query_row(
            "SELECT mtime_ns FROM dirs WHERE path = ?1",
            params![path_s.as_ref()],
            |row| row.get(0),
        )
        .optional()
        .ok()
        .flatten();

    matches!(cached, Some(cached_mtime) if cached_mtime == mtime_ns)
}

fn cache_mark_dir_scanned(conn: &Connection, mode: CacheMode, path: &Path, meta: &std::fs::Metadata) {
    if !mode.allows_write() {
        return;
    }

    let mtime_ns = match modified_time_ns(meta) {
        Some(v) => v,
        None => return,
    };

    let path_s = path.to_string_lossy();
    let ts = now_ns();
    let _ = conn.execute(
        "
        INSERT INTO dirs(path, mtime_ns, last_scanned_ns)
        VALUES(?1, ?2, ?3)
        ON CONFLICT(path) DO UPDATE SET
            mtime_ns = excluded.mtime_ns,
            last_scanned_ns = excluded.last_scanned_ns
        ",
        params![path_s.as_ref(), mtime_ns, ts],
    );
}

fn cache_skip_file(conn: &Connection, mode: CacheMode, path: &Path, meta: &std::fs::Metadata) -> bool {
    if !mode.allows_read() {
        return false;
    }

    let mtime_ns = match modified_time_ns(meta) {
        Some(v) => v,
        None => return false,
    };
    let size = meta.len() as i64;
    let path_s = path.to_string_lossy();

    let cached: Option<(i64, i64)> = conn
        .query_row(
            "SELECT size, mtime_ns FROM files WHERE path = ?1",
            params![path_s.as_ref()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .ok()
        .flatten();

    matches!(cached, Some((cached_size, cached_mtime)) if cached_size == size && cached_mtime == mtime_ns)
}

fn cache_get_last_match(conn: &Connection, path: &Path) -> Option<bool> {
    let path_s = path.to_string_lossy();
    conn.query_row(
        "SELECT last_match FROM files WHERE path = ?1",
        params![path_s.as_ref()],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .ok()
    .flatten()
    .map(|v| v != 0)
}

fn cache_mark_file_seen(conn: &Connection, mode: CacheMode, path: &Path, meta: &std::fs::Metadata) {
    if !mode.allows_write() {
        return;
    }

    let mtime_ns = match modified_time_ns(meta) {
        Some(v) => v,
        None => return,
    };

    let size = meta.len() as i64;
    let dir_path = path.parent().unwrap_or_else(|| Path::new(""));
    let path_s = path.to_string_lossy();
    let dir_s = dir_path.to_string_lossy();
    let ts = now_ns();

    let _ = conn.execute(
        "
        INSERT INTO files(path, dir_path, size, mtime_ns, last_scanned_ns)
        VALUES(?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(path) DO UPDATE SET
            dir_path = excluded.dir_path,
            size = excluded.size,
            mtime_ns = excluded.mtime_ns,
            last_scanned_ns = excluded.last_scanned_ns
        ",
        params![path_s.as_ref(), dir_s.as_ref(), size, mtime_ns, ts],
    );
}

fn cache_record_file_result(
    conn: &Connection,
    mode: CacheMode,
    path: &Path,
    meta: &std::fs::Metadata,
    last_match: bool,
) {
    if !mode.allows_write() {
        return;
    }

    let mtime_ns = match modified_time_ns(meta) {
        Some(v) => v,
        None => return,
    };

    let size = meta.len() as i64;
    let dir_path = path.parent().unwrap_or_else(|| Path::new(""));
    let path_s = path.to_string_lossy();
    let dir_s = dir_path.to_string_lossy();
    let ts = now_ns();
    let match_i = if last_match { 1 } else { 0 };

    let _ = conn.execute(
        "
        INSERT INTO files(path, dir_path, size, mtime_ns, last_scanned_ns, last_match)
        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(path) DO UPDATE SET
            dir_path = excluded.dir_path,
            size = excluded.size,
            mtime_ns = excluded.mtime_ns,
            last_scanned_ns = excluded.last_scanned_ns,
            last_match = excluded.last_match
        ",
        params![path_s.as_ref(), dir_s.as_ref(), size, mtime_ns, ts, match_i],
    );
}

fn should_skip(path: &Path) -> bool {
    let s = path.to_string_lossy().to_lowercase();
    s.contains("\\proc\\")
        || s.ends_with("\\proc")
        || s.contains("/proc/")
        || s.ends_with("/proc")
        || s.contains("\\sys\\")
        || s.ends_with("\\sys")
        || s.contains("/sys/")
        || s.ends_with("/sys")
        || s.contains("\\dev\\")
        || s.ends_with("\\dev")
        || s.contains("/dev/")
        || s.ends_with("/dev")
        || s.contains("\\lost+found\\")
        || s.ends_with("\\lost+found")
        || s.contains("/lost+found/")
        || s.ends_with("/lost+found")
}

fn file_contains(path: &Path, re: &Regex, skipped_large: &AtomicUsize) -> io::Result<bool> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.len() == 0 {
        return Ok(false);
    }

    if let Ok(mmap) = unsafe { Mmap::map(&file) } {
        return Ok(re.is_match(&mmap));
    }

    // Fallback for files that cannot be memory-mapped.
    // Avoid unbounded memory use for very large files.
    const MAX_FALLBACK_SIZE: u64 = 512 * 1024 * 1024; // 512 MB
    if metadata.len() > MAX_FALLBACK_SIZE {
        skipped_large.fetch_add(1, Ordering::Relaxed);
        return Ok(false);
    }

    let mut reader = BufReader::new(file);
    let mut buf = Vec::with_capacity(metadata.len() as usize);
    reader.read_to_end(&mut buf)?;
    Ok(re.is_match(&buf))
}

type Args = (
    String,
    String,
    bool,
    String,
    Option<usize>,
    bool,
    bool,
    bool,
    bool,
    bool,
    usize,
    Option<String>,
    CacheMode,
    bool,
    bool,
    bool,
    bool,
);

fn parse_args() -> Result<Args, String> {
    let mut root = "A:\\".to_string();
    let mut out = "filevector_files.txt".to_string();
    let mut case_sensitive = false;
    let mut pattern = "FileVector\\w*".to_string();
    let mut threads: Option<usize> = None;
    let mut count_first = true;
    let mut verbose = false;
    let mut log_permission_denied = false;
    let mut stop_on_first_error = false;
    let mut match_names = false;
    let mut flush_every: usize = 500;
    let mut cache_path: Option<String> = None;
    let mut cache_mode = CacheMode::Off;
    let mut cache_reset = false;
    let mut cache_unlock = false;
    let mut cache_kill = false;
    let mut cache_local = false;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                root = args.next().ok_or("--root requires a value")?;
            }
            "--out" => {
                out = args.next().ok_or("--out requires a value")?;
            }
            "--pattern" => {
                pattern = args.next().ok_or("--pattern requires a value")?;
            }
            "--case-sensitive" => {
                case_sensitive = true;
            }
            "--threads" => {
                let v = args.next().ok_or("--threads requires a value")?;
                threads = Some(v.parse().map_err(|_| "--threads must be a number")?);
            }
            "--no-count" => {
                count_first = false;
            }
            "--verbose" => {
                verbose = true;
            }
            "--log-permission-denied" => {
                log_permission_denied = true;
            }
            "--stop-on-first-error" => {
                stop_on_first_error = true;
            }
            "--match-names" => {
                match_names = true;
            }
            "--flush-every" => {
                let v = args.next().ok_or("--flush-every requires a value")?;
                flush_every = v.parse().map_err(|_| "--flush-every must be a number")?;
                if flush_every == 0 {
                    return Err("--flush-every must be > 0".to_string());
                }
            }
            "--cache" => {
                cache_path = Some(args.next().ok_or("--cache requires a value")?);
            }
            "--cache-mode" => {
                let v = args.next().ok_or("--cache-mode requires a value")?;
                cache_mode = CacheMode::parse(&v)?;
            }
            "--cache-reset" => {
                cache_reset = true;
            }
            "--cache-unlock" => {
                cache_unlock = true;
            }
            "--cache-kill" => {
                cache_kill = true;
            }
            "--cache-local" => {
                cache_local = true;
            }
            "-h" | "--help" => {
                return Err("help".to_string());
            }
            _ => {
                return Err(format!("unknown arg: {}", arg));
            }
        }
    }

    if cache_local {
        if !is_powershell_env() {
            return Err("--cache-local requires running under PowerShell".to_string());
        }
        cache_path = Some(local_cache_path_string()?);
    }

    if !cache_unlock && cache_mode != CacheMode::Off && cache_path.is_none() {
        return Err("--cache-mode requires --cache PATH".to_string());
    }

    Ok((
        root,
        out,
        case_sensitive,
        pattern,
        threads,
        count_first,
        verbose,
        log_permission_denied,
        stop_on_first_error,
        match_names,
        flush_every,
        cache_path,
        cache_mode,
        cache_reset,
        cache_unlock,
        cache_kill,
        cache_local,
    ))
}

fn print_usage() {
    eprintln!("Usage: file_vector_search [--root PATH] [--out FILE] [--pattern REGEX] [--case-sensitive] [--threads N] [--no-count] [--verbose] [--log-permission-denied] [--stop-on-first-error] [--match-names] [--flush-every N] [--cache PATH] [--cache-mode off|read|write|readwrite] [--cache-reset] [--cache-unlock] [--cache-kill] [--cache-local]");
    eprintln!("Defaults: --root A:\\ --out filevector_files.txt --pattern FileVector\\w*");
}

fn is_powershell_env() -> bool {
    env::var("POWERSHELL_DISTRIBUTION_CHANNEL").is_ok() || env::var("PSModulePath").is_ok()
}

fn local_cache_path_string() -> Result<String, String> {
    let base = env::var("LOCALAPPDATA")
        .map_err(|_| "LOCALAPPDATA is not set; cannot use --cache-local".to_string())?;
    let dir = PathBuf::from(base).join("file_vector_search");
    if let Err(err) = fs::create_dir_all(&dir) {
        return Err(format!("Failed to create cache directory {}: {}", dir.display(), err));
    }
    Ok(dir.join("cache.sqlite").to_string_lossy().into_owned())
}

fn run_cache_kill() -> i32 {
    let current_pid = sysinfo::Pid::from(std::process::id() as usize);
    let refresh = RefreshKind::nothing().with_processes(ProcessRefreshKind::everything());
    let mut system = System::new_with_specifics(refresh);
    system.refresh_processes(ProcessesToUpdate::All, true);

    let mut total = 0;
    let mut killed = 0;
    let mut failed = 0;

    for (pid, proc_) in system.processes() {
        if *pid == current_pid {
            continue;
        }
        let name = proc_.name().to_string_lossy().to_lowercase();
        if name == "file_vector_search.exe" || name == "file_vector_search" {
            total += 1;
            if proc_.kill() {
                killed += 1;
                eprintln!("Cache kill: {} (pid {})", name, pid);
            } else {
                failed += 1;
                eprintln!("Cache kill: {} (pid {}) failed", name, pid);
            }
        }
    }

    if total == 0 {
        eprintln!("Cache kill: no other file_vector_search processes found");
        return 0;
    }
    if failed == 0 {
        eprintln!("Cache kill: terminated {} process(es)", killed);
        0
    } else {
        eprintln!(
            "Cache kill: terminated {} process(es), {} failed",
            killed, failed
        );
        5
    }
}

struct ThreadBuffer {
    buf: Vec<String>,
    out: Arc<Mutex<BufWriter<File>>>,
    flush_every: usize,
}

impl ThreadBuffer {
    fn new(out: Arc<Mutex<BufWriter<File>>>, flush_every: usize) -> Self {
        Self {
            buf: Vec::with_capacity(flush_every),
            out,
            flush_every,
        }
    }

    fn push(&mut self, line: String) {
        self.buf.push(line);
        if self.buf.len() >= self.flush_every {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        if let Ok(mut w) = self.out.lock() {
            for line in self.buf.drain(..) {
                let _ = writeln!(w, "{}", line);
            }
        }
    }
}

impl Drop for ThreadBuffer {
    fn drop(&mut self) {
        self.flush();
    }
}

fn prepare_cache(cache_path: Option<String>, cache_mode: CacheMode, cache_reset: bool) -> Option<CacheConfig> {
    if cache_mode == CacheMode::Off {
        return None;
    }
    let path = PathBuf::from(cache_path?);

    let conn = match Connection::open(&path) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Cache open failed ({}): {}", path.display(), err);
            return None;
        }
    };

    let conn = configure_cache_connection(conn);

    if cache_reset {
        if let Err(err) = reset_cache(&conn) {
            if is_locked_error(&err) {
                eprintln!(
                    "Cache is locked ({}). Close other runs or use a local cache path.",
                    path.display()
                );
                std::process::exit(3);
            }
            eprintln!("Cache reset failed ({}): {}", path.display(), err);
            return None;
        }
        eprintln!("Cache reset: {}", path.display());
    } else if let Err(err) = init_cache_schema(&conn) {
        if is_locked_error(&err) {
            eprintln!(
                "Cache is locked ({}). Close other runs or use a local cache path.",
                path.display()
            );
            std::process::exit(3);
        }
        eprintln!("Cache schema init failed ({}): {}", path.display(), err);
        return None;
    }

    drop(conn);

    Some(CacheConfig {
        path,
        mode: cache_mode,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (
        root,
        out,
        case_sensitive,
        pattern,
        threads,
        count_first,
        verbose,
        log_permission_denied,
        stop_on_first_error,
        match_names,
        flush_every,
        cache_path,
        cache_mode,
        cache_reset,
        cache_unlock,
        cache_kill,
        _cache_local,
    ) = match parse_args() {
        Ok(v) => v,
        Err(e) if e == "help" => {
            print_usage();
            return Ok(());
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            print_usage();
            std::process::exit(2);
        }
    };

    if cache_unlock {
        std::process::exit(run_cache_unlock());
    }
    if cache_kill {
        std::process::exit(run_cache_kill());
    }

    let cache_config = prepare_cache(cache_path, cache_mode, cache_reset);

    if let Some(cfg) = &cache_config {
        eprintln!(
            "Cache enabled: {} (mode: {:?})",
            cfg.path.display(),
            cfg.mode
        );
    }

    let pat = if case_sensitive {
        pattern
    } else {
        format!("(?i:{})", pattern)
    };
    let re = Regex::new(&pat)?;
    let path_re = TextRegex::new(&pat)?;

    let out_path = PathBuf::from(out);
    let out_file = File::create(&out_path)?;
    let out_writer = Arc::new(Mutex::new(BufWriter::new(out_file)));

    let mut total_files: Option<usize> = None;
    let mut count_failed = false;
    if count_first {
        let mut builder = WalkBuilder::new(&root);
        builder
            .hidden(false)
            .git_ignore(false)
            .git_global(false)
            .git_exclude(false)
            .ignore(false)
            .threads(threads.unwrap_or(4));

        let total = AtomicUsize::new(0);
        let count_error_reported = AtomicBool::new(false);
        let count_failed_flag = AtomicBool::new(false);
        let stop_on_first_error = stop_on_first_error;
        let verbose = verbose;
        let cache_config = cache_config.clone();

        builder.build_parallel().run(|| {
            let total = &total;
            let count_error_reported = &count_error_reported;
            let count_failed_flag = &count_failed_flag;
            let stop_on_first_error = stop_on_first_error;
            let verbose = verbose;

            let cache_conn = cache_config
                .as_ref()
                .and_then(|cfg| cfg.open_thread_connection());
            let cache_mode = cache_config.as_ref().map(|cfg| cfg.mode);

            Box::new(move |result| {
                let entry = match result {
                    Ok(e) => e,
                    Err(err) => {
                        if stop_on_first_error || verbose {
                            if !count_error_reported.swap(true, Ordering::Relaxed) {
                                eprintln!("Count pass error: {}", err);
                            }
                            count_failed_flag.store(true, Ordering::Relaxed);
                            return WalkState::Quit;
                        }
                        return WalkState::Continue;
                    }
                };

                let path = entry.path();
                if should_skip(path) {
                    return WalkState::Skip;
                }

                let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
                if is_dir {
                    if let (Some(conn), Some(mode)) = (&cache_conn, cache_mode) {
                        if let Ok(meta) = entry.metadata() {
                            if cache_skip_dir(conn, mode, path, &meta) {
                                return WalkState::Skip;
                            }
                        }
                    }
                    return WalkState::Continue;
                }

                let is_file = entry.file_type().map(|ft| ft.is_file()).unwrap_or(false);
                if !is_file {
                    return WalkState::Continue;
                }

                if let (Some(conn), Some(mode)) = (&cache_conn, cache_mode) {
                    if let Ok(meta) = entry.metadata() {
                        if cache_skip_file(conn, mode, path, &meta) {
                            cache_mark_file_seen(conn, mode, path, &meta);
                            total.fetch_add(1, Ordering::Relaxed);
                            return WalkState::Continue;
                        }
                    }
                }

                total.fetch_add(1, Ordering::Relaxed);
                WalkState::Continue
            })
        });

        count_failed = count_failed_flag.load(Ordering::Relaxed);
        if !count_failed {
            total_files = Some(total.load(Ordering::Relaxed));
            if let Some(t) = total_files {
                eprintln!("Counting done. Files to scan: {}", t);
            }
        }
    }

    if count_failed && stop_on_first_error {
        return Ok(());
    }

    let scanned = AtomicUsize::new(0);
    let matched = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    let skipped_large = AtomicUsize::new(0);
    let perm_denied = AtomicUsize::new(0);
    let first_error_reported = AtomicBool::new(false);

    let mut builder = WalkBuilder::new(&root);
    builder
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false);
    if let Some(t) = threads {
        builder.threads(t);
    }

    let re = Arc::new(re);
    let out_writer = Arc::clone(&out_writer);

    let total_files = total_files;
    let verbose = verbose;
    let log_permission_denied = log_permission_denied;
    let stop_on_first_error = stop_on_first_error;
    let first_error_reported = &first_error_reported;
    let match_names = match_names;
    let path_re = path_re;
    let flush_every = flush_every;
    let cache_config = cache_config.clone();

    builder.build_parallel().run(|| {
        let re = Arc::clone(&re);
        let out_writer = Arc::clone(&out_writer);
        let scanned = &scanned;
        let matched = &matched;
        let errors = &errors;
        let skipped_large = &skipped_large;
        let total_files = total_files;
        let verbose = verbose;
        let log_permission_denied = log_permission_denied;
        let stop_on_first_error = stop_on_first_error;
        let perm_denied = &perm_denied;
        let first_error_reported = first_error_reported;
        let match_names = match_names;
        let path_re = &path_re;
        let flush_every = flush_every;
        let mut out_buf = ThreadBuffer::new(Arc::clone(&out_writer), flush_every);

        let cache_conn = cache_config
            .as_ref()
            .and_then(|cfg| cfg.open_thread_connection());
        let cache_mode = cache_config.as_ref().map(|cfg| cfg.mode);

        Box::new(move |result| {
            let entry = match result {
                Ok(e) => e,
                Err(err) => {
                    if let Some(ioe) = err.io_error() {
                        if ioe.kind() == io::ErrorKind::PermissionDenied {
                            perm_denied.fetch_add(1, Ordering::Relaxed);
                            if (verbose && log_permission_denied) || stop_on_first_error {
                                if !first_error_reported.swap(true, Ordering::Relaxed) {
                                    eprintln!("Walk error: {}", err);
                                }
                                return WalkState::Quit;
                            }
                            return WalkState::Continue;
                        }
                    }
                    errors.fetch_add(1, Ordering::Relaxed);
                    if verbose || stop_on_first_error {
                        if !first_error_reported.swap(true, Ordering::Relaxed) {
                            eprintln!("Walk error: {}", err);
                        }
                        return WalkState::Quit;
                    }
                    return WalkState::Continue;
                }
            };

            let path = entry.path();
            if should_skip(path) {
                return WalkState::Skip;
            }

            let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
            if is_dir {
                if let (Some(conn), Some(mode)) = (&cache_conn, cache_mode) {
                    if let Ok(meta) = entry.metadata() {
                        if cache_skip_dir(conn, mode, path, &meta) {
                            cache_mark_dir_scanned(conn, mode, path, &meta);
                            return WalkState::Skip;
                        }
                        cache_mark_dir_scanned(conn, mode, path, &meta);
                    }
                }
                return WalkState::Continue;
            }

            let is_file = entry.file_type().map(|ft| ft.is_file()).unwrap_or(false);
            if !is_file {
                return WalkState::Continue;
            }

            let meta = entry.metadata();

            if let (Some(conn), Some(mode), Ok(meta_ref)) = (&cache_conn, cache_mode, &meta) {
                if cache_skip_file(conn, mode, path, meta_ref) {
                    cache_mark_file_seen(conn, mode, path, meta_ref);
                    let scanned_now = scanned.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Some(true) = cache_get_last_match(conn, path) {
                        matched.fetch_add(1, Ordering::Relaxed);
                        out_buf.push(path.to_string_lossy().into_owned());
                    }
                    if scanned_now % 1_000 == 0 {
                        if let Some(total) = total_files {
                            let pct = (scanned_now as f64 / total.max(1) as f64) * 100.0;
                            eprintln!("Progress: {}/{} ({:.1}%)", scanned_now, total, pct);
                        } else {
                            eprintln!("Progress: {} files scanned", scanned_now);
                        }
                    }
                    return WalkState::Continue;
                }
            }

            let scanned_now = scanned.fetch_add(1, Ordering::Relaxed) + 1;
            if scanned_now % 1_000 == 0 {
                if let Some(total) = total_files {
                    let pct = (scanned_now as f64 / total.max(1) as f64) * 100.0;
                    eprintln!("Progress: {}/{} ({:.1}%)", scanned_now, total, pct);
                } else {
                    eprintln!("Progress: {} files scanned", scanned_now);
                }
            }

            if match_names && path_re.is_match(&path.to_string_lossy()) {
                matched.fetch_add(1, Ordering::Relaxed);
                out_buf.push(path.to_string_lossy().into_owned());
                if let (Some(conn), Some(mode), Ok(meta_ref)) = (&cache_conn, cache_mode, &meta) {
                    cache_record_file_result(conn, mode, path, meta_ref, true);
                }
                return WalkState::Continue;
            }

            match file_contains(path, &re, skipped_large) {
                Ok(true) => {
                    matched.fetch_add(1, Ordering::Relaxed);
                    out_buf.push(path.to_string_lossy().into_owned());
                    if let (Some(conn), Some(mode), Ok(meta_ref)) = (&cache_conn, cache_mode, &meta) {
                        cache_record_file_result(conn, mode, path, meta_ref, true);
                    }
                }
                Ok(false) => {
                    if let (Some(conn), Some(mode), Ok(meta_ref)) = (&cache_conn, cache_mode, &meta) {
                        cache_record_file_result(conn, mode, path, meta_ref, false);
                    }
                }
                Err(err) => {
                    if err.kind() == io::ErrorKind::PermissionDenied {
                        perm_denied.fetch_add(1, Ordering::Relaxed);
                        if (verbose && log_permission_denied) || stop_on_first_error {
                            if !first_error_reported.swap(true, Ordering::Relaxed) {
                                eprintln!("Read error: {} ({})", path.display(), err);
                            }
                            return WalkState::Quit;
                        }
                        return WalkState::Continue;
                    }
                    errors.fetch_add(1, Ordering::Relaxed);
                    if verbose || stop_on_first_error {
                        if !first_error_reported.swap(true, Ordering::Relaxed) {
                            eprintln!("Read error: {} ({})", path.display(), err);
                        }
                        return WalkState::Quit;
                    }
                }
            }

            WalkState::Continue
        })
    });

    if let Ok(mut w) = out_writer.lock() {
        let _ = w.flush();
    }

    eprintln!(
        "Done. Scanned: {}, matched: {}, errors: {}, skipped_large: {}, permission_denied: {}",
        scanned.load(Ordering::Relaxed),
        matched.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
        skipped_large.load(Ordering::Relaxed),
        perm_denied.load(Ordering::Relaxed)
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use tempfile::TempDir;

    fn write_file(path: &Path, contents: &[u8]) {
        let mut f = File::create(path).expect("create file");
        f.write_all(contents).expect("write file");
    }

    #[test]
    fn should_skip_recognizes_system_like_paths() {
        assert!(should_skip(Path::new(r"A:\proc")));
        assert!(should_skip(Path::new(r"A:\proc\1\maps")));
        assert!(should_skip(Path::new(r"A:\sys\kernel")));
        assert!(should_skip(Path::new(r"A:\dev\tty0")));
        assert!(should_skip(Path::new(r"A:\lost+found")));
        assert!(!should_skip(Path::new(r"A:\users\work\notes.txt")));
    }

    #[test]
    fn file_contains_matches_case_insensitive() {
        let dir = TempDir::new().expect("temp dir");
        let file_path = dir.path().join("a.txt");
        write_file(&file_path, b"hello FileVector123 world");

        let re = Regex::new(r"(?i:FileVector\\w*)").expect("regex");
        let skipped = AtomicUsize::new(0);
        let matched = file_contains(&file_path, &re, &skipped).expect("scan");
        assert!(matched);
        assert_eq!(skipped.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn file_contains_no_match() {
        let dir = TempDir::new().expect("temp dir");
        let file_path = dir.path().join("b.txt");
        write_file(&file_path, b"hello world");

        let re = Regex::new(r"(?i:FileVector\\w*)").expect("regex");
        let skipped = AtomicUsize::new(0);
        let matched = file_contains(&file_path, &re, &skipped).expect("scan");
        assert!(!matched);
    }

    #[test]
    fn main_walk_finds_expected_files_in_temp_root() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path();

        let keep = root.join("keep.txt");
        let miss = root.join("miss.txt");
        write_file(&keep, b"FileVector99 is here");
        write_file(&miss, b"nothing");

        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("mkdir");
        let nested_hit = nested.join("hit.txt");
        write_file(&nested_hit, b"FileVectorX");

        let out_path = root.join("out.txt");
        let out_file = File::create(&out_path).expect("out file");
        let out_writer = Arc::new(Mutex::new(BufWriter::new(out_file)));

        let re = Arc::new(Regex::new(r"(?i:FileVector\\w*)").expect("regex"));
        let scanned = AtomicUsize::new(0);
        let matched = AtomicUsize::new(0);
        let errors = AtomicUsize::new(0);
        let skipped_large = AtomicUsize::new(0);

        let mut builder = WalkBuilder::new(root);
        builder
            .hidden(false)
            .git_ignore(false)
            .git_global(false)
            .git_exclude(false)
            .ignore(false)
            .threads(2);

        let out_writer_cloned = Arc::clone(&out_writer);
        let re_cloned = Arc::clone(&re);

        builder.build_parallel().run(|| {
            let re = Arc::clone(&re_cloned);
            let out_writer = Arc::clone(&out_writer_cloned);
            let scanned = &scanned;
            let matched = &matched;
            let errors = &errors;
            let skipped_large = &skipped_large;

            Box::new(move |result| {
                let entry = match result {
                    Ok(e) => e,
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        return WalkState::Continue;
                    }
                };

                let path = entry.path();
                if should_skip(path) {
                    return WalkState::Skip;
                }

                let is_file = entry.file_type().map(|ft| ft.is_file()).unwrap_or(false);
                if !is_file {
                    return WalkState::Continue;
                }

                scanned.fetch_add(1, Ordering::Relaxed);
                if file_contains(path, &re, skipped_large).unwrap_or(false) {
                    matched.fetch_add(1, Ordering::Relaxed);
                    if let Ok(mut w) = out_writer.lock() {
                        let _ = writeln!(w, "{}", path.display());
                    }
                }

                WalkState::Continue
            })
        });

        assert_eq!(errors.load(Ordering::Relaxed), 0);
        assert_eq!(matched.load(Ordering::Relaxed), 2);

        if let Ok(mut w) = out_writer.lock() {
            let _ = w.flush();
        }
        let out_contents = std::fs::read_to_string(&out_path).expect("read out");
        assert!(out_contents.contains("keep.txt"));
        assert!(out_contents.contains("hit.txt"));
        assert!(!out_contents.contains("miss.txt"));
    }
}

