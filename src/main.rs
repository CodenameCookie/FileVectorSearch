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
use std::sync::mpsc::{self, Sender};
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

fn get_meta_bool(conn: &Connection, key: &str) -> Option<bool> {
    conn.query_row(
        "SELECT value FROM meta WHERE key = ?1",
        params![key],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .ok()
    .flatten()
    .map(|v| v == "1")
}

fn set_meta_bool(conn: &Connection, key: &str, value: bool) {
    let v = if value { "1" } else { "0" };
    let _ = conn.execute(
        "INSERT INTO meta(key, value) VALUES(?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, v],
    );
}

fn runs_db_path() -> PathBuf {
    if let Ok(base) = env::var("LOCALAPPDATA") {
        let dir = PathBuf::from(base).join("file_vector_search");
        let _ = fs::create_dir_all(&dir);
        return dir.join("runs.sqlite");
    }
    PathBuf::from(".file_vector_search_runs.sqlite")
}

fn init_runs_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at_ns INTEGER NOT NULL,
            updated_at_ns INTEGER NOT NULL,
            completed_at_ns INTEGER,
            duration_ns INTEGER,
            status TEXT NOT NULL,
            root TEXT NOT NULL,
            pattern TEXT NOT NULL,
            case_sensitive INTEGER NOT NULL,
            match_names INTEGER NOT NULL,
            threads INTEGER,
            count_first INTEGER NOT NULL,
            out_file TEXT NOT NULL,
            cache_mode TEXT,
            cache_path TEXT,
            cache_local INTEGER NOT NULL,
            cache_enabled INTEGER NOT NULL,
            allow_dir_skip INTEGER NOT NULL,
            continuation_of_id INTEGER,
            is_continuation INTEGER NOT NULL,
            total_files_detected INTEGER NOT NULL DEFAULT 0,
            files_scanned INTEGER NOT NULL DEFAULT 0,
            files_matched INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            permission_denied INTEGER NOT NULL DEFAULT 0,
            skipped_large INTEGER NOT NULL DEFAULT 0,
            dirs_skipped INTEGER NOT NULL DEFAULT 0,
            cache_skipped_files INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at_ns);
        ",
    )?;
    ensure_runs_column(conn, "duration_ns", "INTEGER")?;
    Ok(())
}

fn ensure_runs_column(conn: &Connection, name: &str, decl: &str) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(runs)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        if row? == name {
            return Ok(());
        }
    }
    let sql = format!("ALTER TABLE runs ADD COLUMN {} {}", name, decl);
    conn.execute(&sql, [])?;
    Ok(())
}

#[derive(Clone, Debug)]
struct RunConfig {
    root: String,
    pattern: String,
    case_sensitive: bool,
    match_names: bool,
    threads: Option<usize>,
    count_first: bool,
    out_file: String,
    cache_mode: CacheMode,
    cache_path: Option<String>,
    cache_local: bool,
    allow_dir_skip: bool,
}

#[derive(Debug)]
enum RunEvent {
    TotalFilesInc,
    FilesScannedInc,
    FilesMatchedInc,
    ErrorsInc,
    PermissionDeniedInc,
    SkippedLargeInc,
    DirsSkippedInc,
    CacheSkippedFilesInc,
    SetTotals {
        total_files_detected: i64,
        files_scanned: i64,
        files_matched: i64,
        errors: i64,
        permission_denied: i64,
        skipped_large: i64,
        dirs_skipped: i64,
        cache_skipped_files: i64,
    },
    Finalize { status: &'static str },
}

#[derive(Clone)]
struct RunLogger {
    sender: Sender<RunEvent>,
}

impl RunLogger {
    fn start(config: RunConfig) -> Option<Self> {
        let db_path = runs_db_path();
        let conn = Connection::open(db_path).ok()?;
        init_runs_schema(&conn).ok()?;

        let now = now_ns();
        let cutoff = now - 24 * 60 * 60 * 1_000_000_000i64;

        let signature = (
            config.root.clone(),
            config.pattern.clone(),
            config.match_names,
            config.cache_path.clone().unwrap_or_default(),
        );

        let prev: Option<(i64, i64, Option<i64>, String, String, i64, Option<String>)> = conn
            .query_row(
                "SELECT id, started_at_ns, completed_at_ns, root, pattern, match_names, cache_path FROM runs WHERE started_at_ns >= ?1 ORDER BY id DESC LIMIT 1",
                params![cutoff],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()
            .ok()
            .flatten();

        let mut continuation_of_id: Option<i64> = None;
        let mut is_continuation = false;
        if let Some((prev_id, _, prev_completed, prev_root, prev_pattern, prev_match_names, prev_cache_path)) = prev {
            if prev_root == signature.0
                && prev_pattern == signature.1
                && prev_match_names == signature.2 as i64
                && prev_cache_path.unwrap_or_default() == signature.3
            {
                if prev_completed.is_none() {
                    continuation_of_id = Some(prev_id);
                    is_continuation = true;
                }
            }
        }

        conn.execute(
            "
            INSERT INTO runs(
                started_at_ns, updated_at_ns, status, root, pattern, case_sensitive, match_names, threads, count_first, out_file,
                cache_mode, cache_path, cache_local, cache_enabled, allow_dir_skip, continuation_of_id, is_continuation
            ) VALUES (?1, ?2, 'running', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ",
            params![
                now,
                now,
                config.root,
                config.pattern,
                config.case_sensitive as i64,
                config.match_names as i64,
                config.threads.map(|v| v as i64),
                config.count_first as i64,
                config.out_file,
                match config.cache_mode {
                    CacheMode::Off => "off",
                    CacheMode::Read => "read",
                    CacheMode::Write => "write",
                    CacheMode::ReadWrite => "readwrite",
                },
                config.cache_path,
                config.cache_local as i64,
                (config.cache_mode != CacheMode::Off) as i64,
                config.allow_dir_skip as i64,
                continuation_of_id,
                is_continuation as i64,
            ],
        )
        .ok()?;

        let run_id = conn.last_insert_rowid();
        let run_started_at = now;

        let (tx, rx) = mpsc::channel::<RunEvent>();
        std::thread::spawn(move || {
            let mut totals = (0i64, 0i64, 0i64, 0i64, 0i64, 0i64, 0i64, 0i64);
            let mut status = "running";
            for event in rx {
                match event {
                    RunEvent::TotalFilesInc => totals.0 += 1,
                    RunEvent::FilesScannedInc => totals.1 += 1,
                    RunEvent::FilesMatchedInc => totals.2 += 1,
                    RunEvent::ErrorsInc => totals.3 += 1,
                    RunEvent::PermissionDeniedInc => totals.4 += 1,
                    RunEvent::SkippedLargeInc => totals.5 += 1,
                    RunEvent::DirsSkippedInc => totals.6 += 1,
                    RunEvent::CacheSkippedFilesInc => totals.7 += 1,
                    RunEvent::SetTotals {
                        total_files_detected,
                        files_scanned,
                        files_matched,
                        errors,
                        permission_denied,
                        skipped_large,
                        dirs_skipped,
                        cache_skipped_files,
                    } => {
                        totals = (
                            total_files_detected,
                            files_scanned,
                            files_matched,
                            errors,
                            permission_denied,
                            skipped_large,
                            dirs_skipped,
                            cache_skipped_files,
                        );
                    }
                    RunEvent::Finalize { status: s } => status = s,
                }

                let now = now_ns();
                let _ = conn.execute(
                    "
                    UPDATE runs SET
                        updated_at_ns = ?1,
                        status = ?2,
                        total_files_detected = ?3,
                        files_scanned = ?4,
                        files_matched = ?5,
                        errors = ?6,
                        permission_denied = ?7,
                        skipped_large = ?8,
                        dirs_skipped = ?9,
                        cache_skipped_files = ?10
                    WHERE id = ?11
                    ",
                    params![
                        now,
                        status,
                        totals.0,
                        totals.1,
                        totals.2,
                        totals.3,
                        totals.4,
                        totals.5,
                        totals.6,
                        totals.7,
                        run_id
                    ],
                );

                if status != "running" {
                    let duration = if now >= run_started_at {
                        now - run_started_at
                    } else {
                        0
                    };
                    let _ = conn.execute(
                        "UPDATE runs SET completed_at_ns = ?1, duration_ns = ?2 WHERE id = ?3",
                        params![now, duration, run_id],
                    );
                    break;
                }
            }
        });

        Some(Self { sender: tx })
    }

    fn log(&self, event: RunEvent) {
        let _ = self.sender.send(event);
    }

    fn set_totals(
        &self,
        total_files_detected: i64,
        files_scanned: i64,
        files_matched: i64,
        errors: i64,
        permission_denied: i64,
        skipped_large: i64,
        dirs_skipped: i64,
        cache_skipped_files: i64,
    ) {
        let _ = self.sender.send(RunEvent::SetTotals {
            total_files_detected,
            files_scanned,
            files_matched,
            errors,
            permission_denied,
            skipped_large,
            dirs_skipped,
            cache_skipped_files,
        });
    }

    fn finish(&self, status: &'static str) {
        let _ = self.sender.send(RunEvent::Finalize { status });
    }
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

fn replay_cached_matches(
    conn: &Connection,
    root: &str,
    out_writer: &Arc<Mutex<BufWriter<File>>>,
    run_logger: &Option<RunLogger>,
) -> Result<(i64, i64), String> {
    let prefix = format!("{}%", root);

    let total_files: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE ?1",
            params![prefix.clone()],
            |row| row.get(0),
        )
        .map_err(|err| format!("cache count failed: {}", err))?;

    let mut stmt = conn
        .prepare("SELECT path FROM files WHERE last_match = 1 AND path LIKE ?1")
        .map_err(|err| format!("cache query failed: {}", err))?;

    let rows = stmt
        .query_map(params![prefix], |row| row.get::<_, String>(0))
        .map_err(|err| format!("cache query failed: {}", err))?;

    let mut matched = 0i64;
    if let Ok(mut writer) = out_writer.lock() {
        for row in rows {
            if let Ok(path) = row {
                let _ = writeln!(writer, "{}", path);
                matched += 1;
            }
        }
        let _ = writer.flush();
    }

    if let Some(logger) = run_logger {
        logger.set_totals(
            total_files,
            0,
            matched,
            0,
            0,
            0,
            0,
            total_files,
        );
    }

    Ok((total_files, matched))
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileSearchResult {
    Match,
    NoMatch,
    SkippedLarge,
}

fn file_contains(path: &Path, re: &Regex) -> io::Result<FileSearchResult> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.len() == 0 {
        return Ok(FileSearchResult::NoMatch);
    }

    if let Ok(mmap) = unsafe { Mmap::map(&file) } {
        return Ok(if re.is_match(&mmap) {
            FileSearchResult::Match
        } else {
            FileSearchResult::NoMatch
        });
    }

    // Fallback for files that cannot be memory-mapped.
    // Avoid unbounded memory use for very large files.
    const MAX_FALLBACK_SIZE: u64 = 512 * 1024 * 1024; // 512 MB
    if metadata.len() > MAX_FALLBACK_SIZE {
        return Ok(FileSearchResult::SkippedLarge);
    }

    let mut reader = BufReader::new(file);
    let mut buf = Vec::with_capacity(metadata.len() as usize);
    reader.read_to_end(&mut buf)?;
    Ok(if re.is_match(&buf) {
        FileSearchResult::Match
    } else {
        FileSearchResult::NoMatch
    })
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
    let mut last_run_duration = false;
    let mut last_run_summary = false;
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
            "--last-run-duration" => {
                last_run_duration = true;
            }
            "--last-run-summary" => {
                last_run_summary = true;
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

    let root = normalize_root_path(root);

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
        last_run_duration,
        last_run_summary,
        cache_local,
    ))
}

fn print_usage() {
    eprintln!("Usage: file_vector_search [--root PATH] [--out FILE] [--pattern REGEX] [--case-sensitive] [--threads N] [--no-count] [--verbose] [--log-permission-denied] [--stop-on-first-error] [--match-names] [--flush-every N] [--cache PATH] [--cache-mode off|read|write|readwrite] [--cache-reset] [--cache-unlock] [--cache-kill] [--last-run-duration] [--last-run-summary] [--cache-local]");
    eprintln!("Defaults: --root A:\\ --out filevector_files.txt --pattern FileVector\\w*");
}

fn is_powershell_env() -> bool {
    env::var("POWERSHELL_DISTRIBUTION_CHANNEL").is_ok() || env::var("PSModulePath").is_ok()
}

fn normalize_root_path(root: String) -> String {
    if root.starts_with(r"\\?\") {
        return root;
    }
    if root.starts_with(r"\\") {
        let trimmed = root.trim_start_matches(r"\\");
        return format!(r"\\?\UNC\{}", trimmed);
    }
    root
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

fn run_last_run_duration() -> i32 {
    let db_path = runs_db_path();
    let conn = match Connection::open(db_path) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Failed to open runs DB: {}", err);
            return 2;
        }
    };

    let row: Option<(i64, i64, Option<i64>, Option<i64>, String)> = conn
        .query_row(
            "SELECT id, started_at_ns, completed_at_ns, duration_ns, status FROM runs ORDER BY id DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()
        .unwrap_or(None);

    let Some((id, started_at, completed_at, duration_ns, status)) = row else {
        println!("No runs found.");
        return 0;
    };

    let now = now_ns();
    let duration = if let Some(d) = duration_ns {
        d
    } else if let Some(done) = completed_at {
        if done >= started_at { done - started_at } else { 0 }
    } else if now >= started_at {
        now - started_at
    } else {
        0
    };

    let seconds = duration as f64 / 1_000_000_000.0;
    println!(
        "Last run id: {} | status: {} | duration_ns: {} | duration_sec: {:.3}",
        id, status, duration, seconds
    );
    0
}

fn run_last_run_summary() -> i32 {
    let db_path = runs_db_path();
    let conn = match Connection::open(db_path) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Failed to open runs DB: {}", err);
            return 2;
        }
    };

    let row: Option<(
        i64,
        i64,
        Option<i64>,
        Option<i64>,
        String,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
    )> = conn
        .query_row(
            "SELECT id, started_at_ns, completed_at_ns, duration_ns, status, total_files_detected, files_scanned, files_matched, errors, permission_denied, skipped_large, dirs_skipped FROM runs ORDER BY id DESC LIMIT 1",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                    r.get(9)?,
                    r.get(10)?,
                    r.get(11)?,
                ))
            },
        )
        .optional()
        .unwrap_or(None);

    let Some((
        id,
        started_at,
        completed_at,
        duration_ns,
        status,
        total_files_detected,
        files_scanned,
        files_matched,
        errors,
        permission_denied,
        skipped_large,
        dirs_skipped,
    )) = row else {
        println!("No runs found.");
        return 0;
    };

    let now = now_ns();
    let duration = if let Some(d) = duration_ns {
        d
    } else if let Some(done) = completed_at {
        if done >= started_at { done - started_at } else { 0 }
    } else if now >= started_at {
        now - started_at
    } else {
        0
    };

    let seconds = duration as f64 / 1_000_000_000.0;
    println!(
        "Last run id: {} | status: {} | duration_sec: {:.3} | total_detected: {} | scanned: {} | matched: {} | errors: {} | perm_denied: {} | skipped_large: {} | dirs_skipped: {}",
        id,
        status,
        seconds,
        total_files_detected,
        files_scanned,
        files_matched,
        errors,
        permission_denied,
        skipped_large,
        dirs_skipped
    );
    0
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
        last_run_duration,
        last_run_summary,
        cache_local,
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
    if last_run_duration {
        std::process::exit(run_last_run_duration());
    }
    if last_run_summary {
        std::process::exit(run_last_run_summary());
    }

    let cache_config = prepare_cache(cache_path, cache_mode, cache_reset);

    let mut allow_dir_skip = true;
    if let Some(cfg) = &cache_config {
        if let Some(conn) = cfg.open_thread_connection() {
            let last_complete = get_meta_bool(&conn, "last_run_complete").unwrap_or(false);
            allow_dir_skip = last_complete;
            set_meta_bool(&conn, "last_run_complete", false);
        }
    }

    let run_logger = RunLogger::start(RunConfig {
        root: root.clone(),
        pattern: pattern.clone(),
        case_sensitive,
        match_names,
        threads,
        count_first,
        out_file: out.clone(),
        cache_mode,
        cache_path: cache_config.as_ref().map(|cfg| cfg.path.to_string_lossy().into_owned()),
        cache_local,
        allow_dir_skip,
    });

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

    if cache_mode == CacheMode::Read {
        if let Some(cfg) = &cache_config {
            if let Some(conn) = cfg.open_thread_connection() {
                match replay_cached_matches(&conn, &root, &out_writer, &run_logger) {
                    Ok((total, matched)) => {
                        eprintln!(
                            "Cache replay: matched {} of {} (read-only)",
                            matched, total
                        );
                        if let Some(conn) = cfg.open_thread_connection() {
                            set_meta_bool(&conn, "last_run_complete", true);
                        }
                        if let Some(logger) = &run_logger {
                            logger.finish("completed");
                        }
                        return Ok(());
                    }
                    Err(err) => {
                        eprintln!("Cache replay failed: {}", err);
                    }
                }
            }
        }
    }

    let mut total_files: Option<usize> = None;
    let mut count_failed = false;
    let cache_skipped_files = AtomicUsize::new(0);
    let dirs_skipped = AtomicUsize::new(0);
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
        let allow_dir_skip = allow_dir_skip;
        let dirs_skipped = &dirs_skipped;

        builder.build_parallel().run(|| {
            let total = &total;
            let count_error_reported = &count_error_reported;
            let count_failed_flag = &count_failed_flag;
            let stop_on_first_error = stop_on_first_error;
            let verbose = verbose;
            let dirs_skipped = dirs_skipped;

            let cache_conn = cache_config
                .as_ref()
                .and_then(|cfg| cfg.open_thread_connection());
            let cache_mode = cache_config.as_ref().map(|cfg| cfg.mode);
            let run_logger = run_logger.clone();

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
                    if allow_dir_skip {
                        if let (Some(conn), Some(mode)) = (&cache_conn, cache_mode) {
                            if let Ok(meta) = entry.metadata() {
                                if cache_skip_dir(conn, mode, path, &meta) {
                                    dirs_skipped.fetch_add(1, Ordering::Relaxed);
                                    if let Some(logger) = &run_logger {
                                        logger.log(RunEvent::DirsSkippedInc);
                                    }
                                    return WalkState::Skip;
                                }
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
                            if let Some(logger) = &run_logger {
                                logger.log(RunEvent::TotalFilesInc);
                            }
                            return WalkState::Continue;
                        }
                    }
                }

                total.fetch_add(1, Ordering::Relaxed);
                if let Some(logger) = &run_logger {
                    logger.log(RunEvent::TotalFilesInc);
                }
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
        if let Some(logger) = &run_logger {
            logger.finish("aborted");
        }
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
    let allow_dir_skip = allow_dir_skip;

    builder.build_parallel().run(|| {
        let re = Arc::clone(&re);
        let out_writer = Arc::clone(&out_writer);
        let scanned = &scanned;
        let matched = &matched;
        let errors = &errors;
        let skipped_large = &skipped_large;
        let cache_skipped_files = &cache_skipped_files;
        let dirs_skipped = &dirs_skipped;
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
        let run_logger = run_logger.clone();

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
                            if let Some(logger) = &run_logger {
                                logger.log(RunEvent::PermissionDeniedInc);
                            }
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
                    if let Some(logger) = &run_logger {
                        logger.log(RunEvent::ErrorsInc);
                    }
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
                        if allow_dir_skip && cache_skip_dir(conn, mode, path, &meta) {
                            dirs_skipped.fetch_add(1, Ordering::Relaxed);
                            if let Some(logger) = &run_logger {
                                logger.log(RunEvent::DirsSkippedInc);
                            }
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
                    cache_skipped_files.fetch_add(1, Ordering::Relaxed);
                    if let Some(logger) = &run_logger {
                        logger.log(RunEvent::CacheSkippedFilesInc);
                    }
                    let scanned_now = scanned.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Some(logger) = &run_logger {
                        logger.log(RunEvent::FilesScannedInc);
                    }
                    if let Some(true) = cache_get_last_match(conn, path) {
                        matched.fetch_add(1, Ordering::Relaxed);
                        out_buf.push(path.to_string_lossy().into_owned());
                        if let Some(logger) = &run_logger {
                            logger.log(RunEvent::FilesMatchedInc);
                        }
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
            if let Some(logger) = &run_logger {
                logger.log(RunEvent::FilesScannedInc);
            }
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
                if let Some(logger) = &run_logger {
                    logger.log(RunEvent::FilesMatchedInc);
                }
                if let (Some(conn), Some(mode), Ok(meta_ref)) = (&cache_conn, cache_mode, &meta) {
                    cache_record_file_result(conn, mode, path, meta_ref, true);
                }
                return WalkState::Continue;
            }

            match file_contains(path, &re) {
                Ok(FileSearchResult::Match) => {
                    matched.fetch_add(1, Ordering::Relaxed);
                    out_buf.push(path.to_string_lossy().into_owned());
                    if let Some(logger) = &run_logger {
                        logger.log(RunEvent::FilesMatchedInc);
                    }
                    if let (Some(conn), Some(mode), Ok(meta_ref)) = (&cache_conn, cache_mode, &meta) {
                        cache_record_file_result(conn, mode, path, meta_ref, true);
                    }
                }
                Ok(FileSearchResult::NoMatch) => {
                    if let (Some(conn), Some(mode), Ok(meta_ref)) = (&cache_conn, cache_mode, &meta) {
                        cache_record_file_result(conn, mode, path, meta_ref, false);
                    }
                }
                Ok(FileSearchResult::SkippedLarge) => {
                    skipped_large.fetch_add(1, Ordering::Relaxed);
                    if let Some(logger) = &run_logger {
                        logger.log(RunEvent::SkippedLargeInc);
                    }
                }
                Err(err) => {
                    if err.kind() == io::ErrorKind::PermissionDenied {
                        perm_denied.fetch_add(1, Ordering::Relaxed);
                        if let Some(logger) = &run_logger {
                            logger.log(RunEvent::PermissionDeniedInc);
                        }
                        if (verbose && log_permission_denied) || stop_on_first_error {
                            if !first_error_reported.swap(true, Ordering::Relaxed) {
                                eprintln!("Read error: {} ({})", path.display(), err);
                            }
                            return WalkState::Quit;
                        }
                        return WalkState::Continue;
                    }
                    errors.fetch_add(1, Ordering::Relaxed);
                    if let Some(logger) = &run_logger {
                        logger.log(RunEvent::ErrorsInc);
                    }
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

    if let Some(cfg) = &cache_config {
        if let Some(conn) = cfg.open_thread_connection() {
            set_meta_bool(&conn, "last_run_complete", true);
        }
    }

    if let Some(logger) = &run_logger {
        logger.finish("completed");
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
        let matched = file_contains(&file_path, &re).expect("scan");
        assert_eq!(matched, FileSearchResult::Match);
    }

    #[test]
    fn file_contains_no_match() {
        let dir = TempDir::new().expect("temp dir");
        let file_path = dir.path().join("b.txt");
        write_file(&file_path, b"hello world");

        let re = Regex::new(r"(?i:FileVector\\w*)").expect("regex");
        let matched = file_contains(&file_path, &re).expect("scan");
        assert_eq!(matched, FileSearchResult::NoMatch);
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
                if file_contains(path, &re).map(|v| v == FileSearchResult::Match).unwrap_or(false) {
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

