use data_collect::deployment::CollectorBinaryRole;

#[tokio::main]
async fn main() {
    if let Err(error) =
        data_collect::runtime::run_collector_from_env(CollectorBinaryRole::TrainingCold).await
    {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
