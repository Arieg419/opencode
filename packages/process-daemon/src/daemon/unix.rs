use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use sha2::{Digest, Sha256};

use super::sweep::STALE_RUNTIME_AGE;
use super::{LOCK_FILE, Registration, read_registration, registration_path};
use crate::protocol::PROTOCOL_VERSION;
use crate::transport::Listener;

pub(super) struct Runtime {
    pub registration: Registration,
    pub cleanup: Cleanup,
    sweeper: JoinHandle<()>,
    // Held until the shared server has joined its handlers and removed registration.
    lock: File,
}

impl Runtime {
    pub fn bind(directory: &Path) -> Result<(Self, Listener)> {
        let directory = directory.to_path_buf();
        let lock = lock_runtime(&directory)?;

        let socket_path = socket_path(&directory)?;
        if socket_path.exists() {
            fs::remove_file(&socket_path)?;
        }
        let listener = Listener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        let socket = SocketFile::bound(socket_path)?;
        let registration = Registration {
            instance_id: random_id(),
            pid: std::process::id(),
            protocol: PROTOCOL_VERSION,
            socket: socket.path.clone(),
            token: random_id(),
        };
        write_registration(&directory, &registration)?;
        let sweeper = {
            let directory = directory.clone();
            let socket = socket.path.clone();
            thread::spawn(move || {
                super::sweep::stale_runtimes(&directory);
                stale_sockets(
                    &socket,
                    nix::unistd::Uid::effective().as_raw(),
                    SystemTime::now(),
                );
            })
        };
        Ok((
            Self {
                cleanup: Cleanup {
                    directory,
                    registration: registration.clone(),
                    socket,
                },
                registration,
                sweeper,
                lock,
            },
            listener,
        ))
    }

    /// Removes this daemon's runtime state after the server has joined its handlers.
    pub fn finish(self) -> Result<()> {
        let _ = self.sweeper.join();
        let result = self.cleanup.run();
        drop(self.lock);
        result
    }
}

/// Locks the runtime directory. A sweep or stopping daemon unlinks the lock file
/// while holding it, so a lock taken on an unlinked file proves nothing; retry on
/// the file now at the path.
fn lock_runtime(directory: &Path) -> Result<File> {
    let path = directory.join(LOCK_FILE);
    for _ in 0..3 {
        fs::create_dir_all(directory)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        let lock = match OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(lock) => lock,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        lock.try_lock_exclusive()
            .context("another opencode-pty process already owns the service lock")?;
        if is_current_lock(&lock, &path) {
            return Ok(lock);
        }
    }
    bail!(
        "opencode-pty runtime directory {} kept changing while locking",
        directory.display()
    )
}

pub(super) fn is_current_lock(lock: &File, path: &Path) -> bool {
    match (lock.metadata(), fs::symlink_metadata(path)) {
        (Ok(held), Ok(linked)) => held.dev() == linked.dev() && held.ino() == linked.ino(),
        _ => false,
    }
}

/// Removes old sockets in this daemon's socket root that refuse connections.
/// Their runtime directory may be gone or under another runtime root, where the
/// sibling sweep cannot find them.
fn stale_sockets(own: &Path, uid: u32, now: SystemTime) {
    let Some(entries) = own.parent().and_then(|root| fs::read_dir(root).ok()) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == own || path.extension() != Some("sock".as_ref()) {
            continue;
        }
        if let Err(error) = stale_socket(&path, uid, now) {
            eprintln!(
                "opencode-pty could not remove stale socket {}: {error:#}",
                path.display()
            );
        }
    }
}

fn stale_socket(path: &Path, uid: u32, now: SystemTime) -> Result<()> {
    // Symlinks and other users' files are never removed. The age guard covers a
    // daemon between bind(2) and listen(2), which also refuses connections.
    let before = fs::symlink_metadata(path)?;
    let age = now.duration_since(before.modified()?).unwrap_or_default();
    if !before.file_type().is_socket()
        || before.uid() != uid
        || age < STALE_RUNTIME_AGE
        || !refuses_connections(path)
    {
        return Ok(());
    }
    // A daemon reusing this path replaces the socket first; keep its new one.
    if fs::symlink_metadata(path)
        .is_ok_and(|after| after.dev() == before.dev() && after.ino() == before.ino())
    {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn refuses_connections(path: &Path) -> bool {
    let bytes = path.as_os_str().as_bytes();
    // SAFETY: sockaddr_un is plain data; all zeroes is a valid empty address.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if bytes.len() >= address.sun_path.len() {
        return false;
    }
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (slot, byte) in address.sun_path.iter_mut().zip(bytes) {
        *slot = *byte as libc::c_char;
    }
    // SAFETY: socket returns a new descriptor, adopted immediately.
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return false;
    }
    // SAFETY: fd is a valid descriptor owned by nothing else.
    let socket = unsafe { OwnedFd::from_raw_fd(fd) };
    // Non-blocking, so a live daemon with a full backlog cannot stall the sweep.
    // SAFETY: socket is a live descriptor.
    if unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } != 0 {
        return false;
    }
    // SAFETY: address is a valid sockaddr_un of the given length.
    let result = unsafe {
        libc::connect(
            socket.as_raw_fd(),
            (&raw const address).cast(),
            size_of::<libc::sockaddr_un>() as libc::socklen_t,
        )
    };
    result != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECONNREFUSED)
}

/// Removes only state that still belongs to this daemon instance.
#[derive(Clone)]
pub(super) struct Cleanup {
    directory: PathBuf,
    registration: Registration,
    socket: SocketFile,
}

impl Cleanup {
    pub fn run(&self) -> Result<()> {
        let result = remove_if_current(&self.directory, &self.registration);
        self.socket.remove_if_current();
        // Remove the lock file before releasing it: a daemon that already opened it
        // fails to lock it, and a later one creates a new lock file.
        let _ = fs::remove_file(self.directory.join(LOCK_FILE));
        let _ = fs::remove_dir(&self.directory);
        result
    }
}

/// The bound socket's inode, so cleanup never unlinks a successor's socket.
#[derive(Clone)]
struct SocketFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketFile {
    fn bound(path: PathBuf) -> Result<Self> {
        let metadata = fs::symlink_metadata(&path)?;
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn remove_if_current(&self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(super) fn process_exists(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal zero only checks whether the PID exists.
    let exists = unsafe { libc::kill(pid, 0) } == 0;
    exists || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Removes the socket of an abandoned runtime found by the shared sweep.
pub(super) fn remove_stale_endpoint(
    canonical_directory: &Path,
    registration: Option<&Registration>,
) -> Result<()> {
    // The abandoned daemon may have used a different OPENCODE_PTY_SOCKET_DIR, so
    // prefer the socket it registered over this daemon's socket root.
    let socket = match registration {
        Some(registration) => registration.socket.clone(),
        None => socket_file(&socket_root()?, canonical_directory),
    };
    let uid = nix::unistd::Uid::effective().as_raw();
    if fs::symlink_metadata(&socket)
        .is_ok_and(|metadata| metadata.file_type().is_socket() && metadata.uid() == uid)
    {
        fs::remove_file(&socket)?;
    }
    Ok(())
}

fn socket_path(directory: &Path) -> Result<PathBuf> {
    let directory =
        fs::canonicalize(directory).context("failed to resolve PTY runtime directory")?;
    let root = socket_root()?;
    ensure_private_directory(&root)?;
    Ok(socket_file(&root, &directory))
}

// The default path stays short enough for sun_path; temp cleaners skip sockets.
fn socket_root() -> Result<PathBuf> {
    resolve_socket_root(
        std::env::var_os("OPENCODE_PTY_SOCKET_DIR").map(PathBuf::from),
        nix::unistd::Uid::effective().as_raw(),
    )
}

fn resolve_socket_root(directory: Option<PathBuf>, uid: u32) -> Result<PathBuf> {
    let base = match directory {
        None => PathBuf::from("/tmp"),
        Some(directory) if !directory.is_absolute() => {
            bail!("OPENCODE_PTY_SOCKET_DIR must be an absolute path")
        }
        Some(directory)
            if directory
                .components()
                .any(|part| part == Component::ParentDir) =>
        {
            bail!("OPENCODE_PTY_SOCKET_DIR must not contain '..'")
        }
        Some(directory) => directory,
    };
    // Sockets live in a dedicated child so the 0700 permission change never
    // applies to a caller-chosen directory.
    Ok(base.join(format!("opencode-pty-{uid}")))
}

fn socket_file(root: &Path, canonical_directory: &Path) -> PathBuf {
    let digest = Sha256::digest(canonical_directory.as_os_str().as_bytes());
    let name = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    root.join(format!("{name}.sock"))
}

fn ensure_private_directory(directory: &Path) -> Result<()> {
    fs::create_dir_all(directory)?;
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "PTY socket directory is not a real directory: {}",
            directory.display()
        ));
    }
    let uid = nix::unistd::Uid::effective().as_raw();
    if metadata.uid() != uid {
        return Err(anyhow!(
            "PTY socket directory {} is owned by uid {}, expected {uid}",
            directory.display(),
            metadata.uid()
        ));
    }
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn write_registration(directory: &Path, registration: &Registration) -> Result<()> {
    let temporary = directory.join(format!("service.{}.tmp", registration.instance_id));
    let data = serde_json::to_vec_pretty(registration)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&data)?;
    file.sync_all()?;
    fs::rename(&temporary, registration_path(directory))?;
    Ok(())
}

fn remove_if_current(directory: &Path, registration: &Registration) -> Result<()> {
    if read_registration(directory)
        .is_ok_and(|current| current.instance_id == registration.instance_id)
    {
        fs::remove_file(registration_path(directory))?;
    }
    Ok(())
}

fn random_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;

    use super::*;

    #[test]
    fn socket_root_preserves_default_uid_directory() {
        assert_eq!(
            resolve_socket_root(None, 12345).unwrap(),
            PathBuf::from("/tmp/opencode-pty-12345")
        );
    }

    #[test]
    fn socket_root_uses_dedicated_child_of_explicit_directory() {
        assert_eq!(
            resolve_socket_root(Some(PathBuf::from("/private/tmp/task-owned-pty")), 12345).unwrap(),
            PathBuf::from("/private/tmp/task-owned-pty/opencode-pty-12345")
        );
    }

    #[test]
    fn socket_root_rejects_relative_and_parent_overrides() {
        for root in [
            "",
            ".",
            "..",
            "relative",
            "../socket",
            "/tmp/..",
            "/tmp/a/../b",
        ] {
            assert!(resolve_socket_root(Some(PathBuf::from(root)), 12345).is_err());
        }
    }

    #[test]
    fn socket_root_leaves_explicit_directory_permissions_alone() {
        let base = std::env::temp_dir().join(format!("opencode-pty-root-test-{}", random_id()));
        fs::create_dir_all(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();

        let root = resolve_socket_root(Some(base.clone()), 12345).unwrap();
        ensure_private_directory(&root).unwrap();

        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&base), 0o755);
        assert_eq!(mode(&root), 0o700);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn socket_root_rejects_symlinked_child() {
        let base = std::env::temp_dir().join(format!("opencode-pty-root-test-{}", random_id()));
        fs::create_dir_all(base.join("target")).unwrap();
        let root = resolve_socket_root(Some(base.clone()), 12345).unwrap();
        std::os::unix::fs::symlink(base.join("target"), &root).unwrap();
        assert!(ensure_private_directory(&root).is_err());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn socket_paths_are_short_and_runtime_specific() {
        let base = std::env::temp_dir().join(format!("opencode-pty-socket-test-{}", random_id()));
        let first = base.join("a".repeat(120)).join("database-a");
        let second = base.join("b".repeat(120)).join("database-b");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();

        let first_socket = socket_path(&first).unwrap();
        let second_socket = socket_path(&second).unwrap();

        assert_ne!(first_socket, second_socket);
        assert!(first_socket.as_os_str().as_bytes().len() < 104);
        assert_eq!(first_socket.parent(), second_socket.parent());

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn lock_is_current_only_while_linked_at_its_path() {
        let directory =
            std::env::temp_dir().join(format!("opencode-pty-lock-test-{}", random_id()));
        let lock = lock_runtime(&directory).unwrap();
        let path = directory.join(LOCK_FILE);
        assert!(is_current_lock(&lock, &path));
        assert!(lock_runtime(&directory).is_err());

        fs::remove_file(&path).unwrap();
        assert!(!is_current_lock(&lock, &path));
        fs::write(&path, b"").unwrap();
        assert!(!is_current_lock(&lock, &path));
        // The replacement lock file is free, so a new daemon can claim it.
        let successor = lock_runtime(&directory).unwrap();
        assert!(is_current_lock(&successor, &path));
        fs::remove_dir_all(directory).unwrap();
    }

    struct SocketRoot(PathBuf);

    impl SocketRoot {
        fn new() -> Self {
            // macOS temp directories are too long for sun_path.
            let root = PathBuf::from(format!("/tmp/opencode-pty-test-{}", &random_id()[..16]));
            fs::create_dir(&root).unwrap();
            Self(root)
        }

        fn dead(&self, name: &str) -> PathBuf {
            let path = self.0.join(name);
            drop(UnixListener::bind(&path).unwrap());
            path
        }
    }

    impl Drop for SocketRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn later() -> SystemTime {
        SystemTime::now() + STALE_RUNTIME_AGE + std::time::Duration::from_secs(1)
    }

    fn uid() -> u32 {
        nix::unistd::Uid::effective().as_raw()
    }

    #[test]
    fn socket_sweep_removes_only_old_dead_sockets() {
        let root = SocketRoot::new();
        let own = root.dead("own.sock");
        let dead = root.dead("dead.sock");
        let live = root.0.join("live.sock");
        let listener = UnixListener::bind(&live).unwrap();
        let other = root.dead("other.socket");
        let file = root.0.join("file.sock");
        fs::write(&file, b"").unwrap();

        stale_sockets(&own, uid(), later());

        assert!(own.exists());
        assert!(!dead.exists());
        assert!(live.exists());
        assert!(other.exists());
        assert!(file.exists());
        drop(listener);
    }

    #[test]
    fn socket_sweep_keeps_young_foreign_and_symlinked_sockets() {
        let root = SocketRoot::new();
        let own = root.0.join("own.sock");
        let young = root.dead("young.sock");
        let target = root.dead("target");
        let link = root.0.join("link.sock");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        stale_sockets(&own, uid(), SystemTime::now());
        assert!(young.exists());
        stale_sockets(&own, uid() + 1, later());
        assert!(young.exists());
        stale_sockets(&own, uid(), later());

        assert!(!young.exists());
        assert!(fs::symlink_metadata(&link).is_ok());
        assert!(target.exists());
    }

    #[test]
    fn sweep_removes_abandoned_socket() {
        let root = std::env::temp_dir().join(format!("opencode-pty-sweep-test-{}", random_id()));
        let abandoned = root.join(random_id());
        fs::create_dir_all(&abandoned).unwrap();
        fs::write(abandoned.join(LOCK_FILE), b"").unwrap();
        let socket = socket_path(&abandoned).unwrap();
        drop(UnixListener::bind(&socket).unwrap());

        let later = std::time::SystemTime::now()
            + super::super::sweep::STALE_RUNTIME_AGE
            + std::time::Duration::from_secs(1);
        super::super::sweep::runtime(&abandoned, later).unwrap();

        assert!(!abandoned.exists());
        assert!(!socket.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sweep_removes_socket_recorded_in_registration() {
        let root = std::env::temp_dir().join(format!("opencode-pty-sweep-test-{}", random_id()));
        let abandoned = root.join(random_id());
        fs::create_dir_all(&abandoned).unwrap();
        fs::write(abandoned.join(LOCK_FILE), b"").unwrap();
        // macOS temp directories are too long for sun_path.
        let socket = PathBuf::from(format!(
            "/tmp/opencode-pty-test-{}.sock",
            &random_id()[..16]
        ));
        drop(UnixListener::bind(&socket).unwrap());
        let registration = Registration {
            instance_id: random_id(),
            pid: std::process::id(),
            protocol: PROTOCOL_VERSION,
            socket: socket.clone(),
            token: random_id(),
        };
        write_registration(&abandoned, &registration).unwrap();

        let later = std::time::SystemTime::now()
            + super::super::sweep::STALE_RUNTIME_AGE
            + std::time::Duration::from_secs(1);
        super::super::sweep::runtime(&abandoned, later).unwrap();

        assert!(!abandoned.exists());
        assert!(!socket.exists());
        fs::remove_dir_all(root).unwrap();
    }
}
