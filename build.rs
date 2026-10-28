use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!(
        "cargo:rustc-env=OVERGRAPH_BUILD_TARGET={}",
        env::var("TARGET").expect("Cargo build target")
    );
    // Record identity, not flags or a new compiler profile. Default compilation
    // remains available with other compilers; candidate layout validation refuses
    // an unknown identity before its governed allocation is constructed.
    let compiler = env::var_os("RUSTC").expect("Cargo compiler");
    let identity = Command::new(compiler)
        .arg("-Vv")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .unwrap_or_default();
    for (field, key) in [
        ("commit-hash: ", "OVERGRAPH_BUILD_RUSTC_COMMIT"),
        ("release: ", "OVERGRAPH_BUILD_RUSTC_RELEASE"),
        ("LLVM version: ", "OVERGRAPH_BUILD_LLVM_VERSION"),
    ] {
        let value = identity
            .lines()
            .find_map(|line| line.strip_prefix(field))
            .unwrap_or("unavailable");
        println!("cargo:rustc-env={key}={value}");
    }
}
