use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string());

    let build_time = {
        let now = chrono::Local::now();
        now.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
    };

    let git_commit = get_git_commit().unwrap_or_else(|| "unknown".to_string());

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

    println!("cargo:rerun-if-changed=build.rs");
}

fn get_git_commit() -> Option<String> {
    use std::process::Command;

    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;

    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}
