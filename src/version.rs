pub mod version_info {
    include!(concat!(env!("OUT_DIR"), "/version_info.rs"));
}

pub fn get_full_version() -> String {
    format!(
        "{} (commit: {}, built: {})",
        version_info::VERSION,
        version_info::GIT_COMMIT,
        version_info::BUILD_TIME
    )
}
