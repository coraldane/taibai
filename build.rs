use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let version = env::var("TAIBAI_VERSION")
        .or_else(|_| env::var("CARGO_PKG_VERSION"))
        .unwrap_or_else(|_| "unknown".to_string());

    let build_time = env::var("TAIBAI_BUILD_TIME").unwrap_or_else(|_| {
        let now = chrono::Local::now();
        now.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
    });

    let git_commit = env::var("TAIBAI_GIT_COMMIT")
        .or_else(|_| get_git_commit())
        .unwrap_or_else(|_| "unknown".to_string());

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("version_info.rs");

    let content = format!(
        r#"pub const VERSION: &str = "{}";
pub const BUILD_TIME: &str = "{}";
pub const GIT_COMMIT: &str = "{}";
"#,
        version, build_time, git_commit
    );

    fs::write(&dest_path, content).unwrap();

    println!("cargo:rerun-if-env-changed=TAIBAI_VERSION");
    println!("cargo:rerun-if-env-changed=TAIBAI_BUILD_TIME");
    println!("cargo:rerun-if-env-changed=TAIBAI_GIT_COMMIT");
    println!("cargo:rerun-if-changed=build.rs");

    println!("cargo:rustc-env=TAIBAI_VERSION={}", version);
    println!("cargo:rustc-env=TAIBAI_BUILD_TIME={}", build_time);
    println!("cargo:rustc-env=TAIBAI_GIT_COMMIT={}", git_commit);
}

fn get_git_commit() -> Result<String, env::VarError> {
    use std::process::Command;

    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok();

    match output {
        Some(o) if o.status.success() => {
            let commit = String::from_utf8_lossy(&o.stdout).trim().to_string();
            Ok(commit)
        }
        _ => Err(env::VarError::NotPresent),
    }
}
