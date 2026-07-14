use std::ffi::OsStr;

fn main() {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() == 1 && matches!(args[0].to_str(), Some("--help" | "-h")) {
        println!("{}", data_collect::runtime::TRAINING_COLLECTOR_USAGE);
        return;
    }
    if args.len() == 1 && args[0] == OsStr::new("--version") {
        println!("{}", data_collect::runtime::training_collector_version());
        return;
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to create Tokio runtime: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = runtime.block_on(data_collect::runtime::run_training_collector(args)) {
        eprintln!("{error}");
        std::process::exit(data_collect::runtime::collector_exit_code(error.as_ref()));
    }
}
