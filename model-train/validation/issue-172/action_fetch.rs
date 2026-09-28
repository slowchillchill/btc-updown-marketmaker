//! 用正式宿主实际加载的 C 动态库验证 OutputSelector 的执行节点；独立诊断，不计入正式延迟。
use ort::{
    session::{OutputSelector, RunOptions, Session},
    value::Tensor,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::PathBuf};

fn main() {
    let graphs = PathBuf::from(std::env::var("FRAME_GRAPHS").unwrap());
    let out = PathBuf::from(std::env::var("FETCH_OUTPUT").unwrap());
    std::fs::create_dir(&out).unwrap();
    ort::init_from(std::env::var("ORT_DYLIB_PATH").unwrap())
        .unwrap()
        .commit();
    for (name, width, threads) in [("MAIN-1x256", 256usize, 4usize), ("MAIN-30x1024", 1024, 16)] {
        let mut calls = Vec::new();
        let mut original = None;
        for fetch in [None, Some("family_logits"), Some("cancel_embeddings")] {
            let label = fetch.unwrap_or("all");
            let mut session = Session::builder()
                .unwrap()
                .with_optimization_level(ort::session::builder::GraphOptimizationLevel::All)
                .unwrap()
                .with_execution_providers([ort::ep::CPU::default()
                    .with_arena_allocator(true)
                    .build()])
                .unwrap()
                .with_intra_threads(threads)
                .unwrap()
                .with_inter_threads(1)
                .unwrap()
                .with_parallel_execution(false)
                .unwrap()
                .with_memory_pattern(false)
                .unwrap()
                .with_intra_op_spinning(false)
                .unwrap()
                .with_inter_op_spinning(false)
                .unwrap()
                .with_profiling(out.join(format!("{name}-{label}")))
                .unwrap()
                .commit_from_file(graphs.join(name).join("action_decoder.onnx"))
                .unwrap();
            let selection = fetch.map_or_else(OutputSelector::default, |v| {
                OutputSelector::no_default().with(v)
            });
            let options = RunOptions::new().unwrap().with_outputs(selection);
            let outputs = session
                .run_with_options(
                    ort::inputs![
                "z_t"=>Tensor::from_array(([1,width],vec![0f32;width])).unwrap(),
                "rule_features"=>Tensor::from_array(([1,30],vec![0f32;30])).unwrap(),
                "prefix_features"=>Tensor::from_array(([1,60],vec![0f32;60])).unwrap(),
                "cancel_features"=>Tensor::from_array(([1,1,184],vec![0f32;184])).unwrap(),
                "cancel_mask"=>Tensor::from_array(([1,1],vec![true])).unwrap(),
                "cancel_reduced"=>Tensor::from_array(([1,3*width/2],vec![0f32;3*width/2])).unwrap(),
                "cancel_exists"=>Tensor::from_array(([1],vec![true])).unwrap(),
                "price_fee_features"=>Tensor::from_array(([1,9999],vec![0f32;9999])).unwrap()],
                    &options,
                )
                .unwrap();
            let returned = outputs.keys().map(str::to_owned).collect::<Vec<_>>();
            drop(outputs);
            let profile = PathBuf::from(session.end_profiling().unwrap());
            let events: Vec<Value> =
                serde_json::from_slice(&std::fs::read(&profile).unwrap()).unwrap();
            let nodes = events
                .into_iter()
                .filter(|v| {
                    v["cat"] == "Node" && v["name"].as_str().unwrap_or("").ends_with("_kernel_time")
                })
                .map(|v| json!({"name":v["name"],"op":v["args"]["op_name"],"us":v["dur"]}))
                .collect::<Vec<_>>();
            let names = nodes
                .iter()
                .map(|v| v["name"].as_str().unwrap().to_owned())
                .collect::<BTreeSet<_>>();
            let same = original.as_ref().is_none_or(|want| *want == names);
            if original.is_none() {
                original = Some(names);
            }
            calls.push(json!({"requested":fetch,"returned":returned,"nodes":nodes,"same_executed_names_as_all":same,"raw_profile":profile.file_name().unwrap().to_str().unwrap()}));
        }
        std::fs::write(out.join(format!("{name}-native-fetch.json")),serde_json::to_vec_pretty(&json!({"model":name,"threads":threads,"ort_build":ort::info(),"interface":"Rust ort 2.0.0-rc.13 OutputSelector, C API 27","python_only_execute_path_to_fetches_flag":false,"diagnostic_only":true,"calls":calls})).unwrap()).unwrap();
    }
}
