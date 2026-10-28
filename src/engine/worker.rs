//! Startup policy for the two governed candidate workers.
//!
//! This is the pinned std built-in default on all four supported targets. An
//! explicit request skips std's uncapped RUST_MIN_STACK copy/cache, intentionally
//! ignoring ambient overrides for these private workers. OS rounding/guards and
//! mapped-versus-resident stack accounting remain std/OS responsibilities.

pub(super) const STACK_BYTES: usize = 2 * 1024 * 1024;

pub(super) fn builder(name: &str) -> std::thread::Builder {
    std::thread::Builder::new()
        .name(name.into())
        .stack_size(STACK_BYTES)
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    // A standalone main, never libtest: its measured spawn is the first one in
    // each process. Include the actual std-only helper, with no public test API.
    const PROBE: &str = r#"
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
static ACTIVE: AtomicBool = AtomicBool::new(false);
static REQUESTS: AtomicUsize = AtomicUsize::new(0);
static ENV_REQUESTS: AtomicUsize = AtomicUsize::new(0);
static MAX_REQUEST: AtomicUsize = AtomicUsize::new(0);
struct ObservedSystem;
fn note(size: usize) {
    if ACTIVE.load(Ordering::Relaxed) {
        REQUESTS.fetch_add(1, Ordering::Relaxed);
        if size == 65_537 { ENV_REQUESTS.fetch_add(1, Ordering::Relaxed); }
        MAX_REQUEST.fetch_max(size, Ordering::Relaxed);
    }
}
// SAFETY: test-only request observation delegates original valid arguments to
// System; hooks allocate nothing and neither dereference nor change ownership.
unsafe impl GlobalAlloc for ObservedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() { note(layout.size()); }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() { note(layout.size()); }
        p
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(p, layout, size) };
        if !result.is_null() { note(size); }
        result
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: ObservedSystem = ObservedSystem;
fn main() {
    // Argument and output fixtures are outside the interval; never read the
    // environment, spawn a fixture worker, or initialize std's stack cache.
    let mode = std::env::args().nth(1).unwrap();
    let role = std::env::args().nth(2).unwrap();
    let name = match role.as_str() {
        "reclaimer" => "overgraph-reclaimer",
        "coordinator" => "overgraph-slice-coordinator",
        _ => panic!("unknown fixture role"),
    };
    ACTIVE.store(true, Ordering::Relaxed);
    let builder = if mode == "helper" {
        worker::builder(name)
    } else {
        assert_eq!(mode, "control");
        std::thread::Builder::new().name(name.into())
    };
    builder.spawn(|| {}).unwrap().join().unwrap();
    ACTIVE.store(false, Ordering::Relaxed);
    println!("requests={} environment_length_requests={} max_request={} helper_policy_stack_bytes={}",
        REQUESTS.load(Ordering::Relaxed), ENV_REQUESTS.load(Ordering::Relaxed),
        MAX_REQUEST.load(Ordering::Relaxed), worker::STACK_BYTES);
}
"#;

    #[test]
    fn p45a2_worker_first_spawn_skips_oversized_environment_copy() {
        let version = Command::new("rustc").arg("-vV").output().unwrap();
        assert!(version.status.success());
        let identity = String::from_utf8(version.stdout).unwrap();
        for (key, value) in [
            ("release", env!("OVERGRAPH_BUILD_RUSTC_RELEASE")),
            ("commit-hash", env!("OVERGRAPH_BUILD_RUSTC_COMMIT")),
            ("LLVM version", env!("OVERGRAPH_BUILD_LLVM_VERSION")),
        ] {
            assert!(identity
                .lines()
                .any(|line| line == format!("{key}: {value}")));
        }
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("first_spawn.rs");
        let executable = dir.path().join("first_spawn");
        let helper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine/worker.rs");
        let program = format!("#[path = {:?}] mod worker;\n{PROBE}", helper);
        std::fs::write(&source, &program).unwrap();
        let profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        let flags: &[&str] = if cfg!(debug_assertions) {
            &[
                "-Copt-level=0",
                "-Cdebuginfo=2",
                "-Cdebug-assertions=yes",
                "-Ccodegen-units=256",
            ]
        } else {
            &[
                "-Copt-level=3",
                "-Clto=yes",
                "-Ccodegen-units=1",
                "-Cstrip=symbols",
                "-Cdebug-assertions=no",
            ]
        };
        let output = Command::new("rustc")
            .arg("--edition=2021")
            .args(flags)
            .arg(&source)
            .arg("-o")
            .arg(&executable)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        println!("standalone first-spawn compiler={identity} profile={profile} flags={flags:?}");
        let oversized = "x".repeat(65_537);
        for role in ["reclaimer", "coordinator"] {
            for mode in ["helper", "control"] {
                for malformed in [false, true] {
                    let mut child = Command::new(&executable);
                    child.args([mode, role]);
                    if malformed {
                        child.env("RUST_MIN_STACK", &oversized);
                    } else {
                        child.env_remove("RUST_MIN_STACK");
                    }
                    let result = child.output().unwrap();
                    assert!(
                        result.status.success(),
                        "{}",
                        String::from_utf8_lossy(&result.stderr)
                    );
                    let result = String::from_utf8(result.stdout).unwrap();
                    let value = |field: &str| -> usize {
                        result
                            .split_whitespace()
                            .find_map(|part| part.strip_prefix(field).map(|s| s.parse().unwrap()))
                            .unwrap()
                    };
                    let copies = value("environment_length_requests=");
                    if mode == "control" && malformed {
                        assert!(
                            copies > 0,
                            "fresh default control failed to observe copy: {result}"
                        );
                        assert!(value("max_request=") >= 65_537);
                    } else {
                        assert_eq!(copies, 0, "{result}");
                        assert!(value("max_request=") < 64 * 1024, "{result}");
                    }
                    assert_eq!(value("helper_policy_stack_bytes="), super::STACK_BYTES);
                    println!("role={role} mode={mode} malformed={malformed} {result}");
                }
            }
        }
        // Both actual candidate spawn sites must use this helper. The standalone
        // allocator probe alone would miss an accidentally unwired worker.
        for (source, name) in [
            (include_str!("reclamation.rs"), "overgraph-reclaimer"),
            (
                include_str!("append_slice.rs"),
                "overgraph-slice-coordinator",
            ),
        ] {
            if name == "overgraph-slice-coordinator" {
                // Configured coordinators now pass their name through the
                // completion authority, which applies the same stack helper.
                assert!(source.contains(name));
                assert!(include_str!("completion.rs").contains("super::worker::builder(name)"));
            } else {
                assert!(source.contains(&format!("super::worker::builder(\"{name}\")")));
            }
        }
        if let Some(destination) = std::env::var_os("OVERGRAPH_P45A2_WORKER_PROBE_ARTIFACTS") {
            let destination = std::path::PathBuf::from(destination).join(profile);
            std::fs::create_dir_all(&destination).unwrap();
            // Preserve source, helper and ordinary probe binary for command/hash
            // evidence, outside the repo. This is not a numerical stack certificate.
            std::fs::copy(&source, destination.join("first_spawn.rs")).unwrap();
            std::fs::copy(&helper, destination.join("worker.rs")).unwrap();
            std::fs::copy(&executable, destination.join("first_spawn")).unwrap();
        }
    }
}
