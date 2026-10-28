fn main() {
    if let Err(error) =
        data_pipeline_training::historical_admission::run_cli(std::env::args().skip(1))
    {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
