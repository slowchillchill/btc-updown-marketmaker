//! 既有结果读出专用入口，不调用模型或账户运行入口。
fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        eprintln!("用法: model_result_readout <future-execution目录> <新的输出目录>");
        std::process::exit(2);
    }
    if let Err(error) = model_train::result_readout::read_existing_package(
        std::path::Path::new(&args[0]),
        std::path::Path::new(&args[1]),
    ) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
