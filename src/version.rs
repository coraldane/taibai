pub mod version_info {
    include!(concat!(env!("OUT_DIR"), "/version_info.rs"));
}

pub fn get_version() -> &'static str {
    version_info::VERSION
}

pub fn get_build_time() -> &'static str {
    version_info::BUILD_TIME
}

pub fn get_git_commit() -> &'static str {
    version_info::GIT_COMMIT
}

pub fn get_full_version() -> String {
    format!(
        "{} (commit: {}, built: {})",
        version_info::VERSION,
        version_info::GIT_COMMIT,
        version_info::BUILD_TIME
    )
}

pub fn print_version() {
    println!("Monax {}", get_full_version());
    println!("Target: {}", version_info::TARGET);
    println!("Profile: {}", version_info::PROFILE);
}
