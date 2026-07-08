fn main() {
    if let Err(error) = data_collect::retention_admin::run_from_env() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
