//! 有限真实跨UTC验证的冷配置；业务准备仍由原compact CLI与HistoricalMerge执行。
use std::{fs::File,path::PathBuf,time::{Duration,Instant}};
use data_pipeline_training::{compact_public::CompactPublicPreparationConfigV1,
    historical_preparation_progress::{HistoricalPreparationNotifierV1,HistoricalPreparationNoticeV1}};

fn main()->Result<(),Box<dyn std::error::Error+Send+Sync>> {
    let args=std::env::args().collect::<Vec<_>>();
    if args[1]=="--verify-initialization" {
        use data_pipeline_training::historical_initialization::*;
        use data_pipeline_core::{public_input::*,public_reference::source_evidence_v3_from_typed_raw_v2};
        let config:CompactPublicPreparationConfigV1=serde_json::from_reader(File::open(&args[2])?)?;
        let selection=MetadataInitializationSelectionV1::open(config.initialization.as_ref().unwrap(),
            config.canonical.decode_batch_rows,config.canonical.max_decode_batch_bytes)?;
        let mut found=0;
        for entry in selection.read(config.canonical.decode_batch_rows,config.canonical.max_decode_batch_bytes) {
            let (row,binding)=entry?;
            let metadata=match &row {
                TypedRawRowV2::PolymarketMarketMetadataV6(row)=>match row.as_ref() {
                    PolymarketMarketMetadataV6Input::Existing(row)=>row,_=>continue,
                },_=>continue,
            };
            if metadata.condition_id!=args[3] {continue;}
            let source=source_evidence_v3_from_typed_raw_v2(&row)?;
            assert!(validate_metadata_initial_identity_v1(&row)?);
            assert_eq!(binding.as_ref().unwrap().source_message_id,source.source_message_id);
            // 同一真实身份行作为切片首条时，不再要求另一份前序身份。
            let first=select_prefix_initialization_config_v1(&config.files,
                &config.initialization.as_ref().unwrap().files[0].manifest_path,
                metadata.timestamp_received,metadata.timestamp_received+1,
                config.canonical.decode_batch_rows,config.canonical.max_decode_batch_bytes)?;
            assert!(!first.markets.iter().any(|market|market.condition_id.0==args[3]));
            println!("{}",serde_json::json!({"condition":metadata.condition_id,"source":source,
                "binding":binding,"first_row_requires_predecessor":false}));
            found+=1;
        }
        assert_eq!(found,1);
        // 仅本有限代表核两条真实原行的采集引用关联，不加入生产选择器。
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use data_collect::polymarket_market_metadata_v6_layout::polymarket_market_metadata_v6_row_from_batch;
        let member=PathBuf::from(&args[4]);
        assert!(config.files.iter().any(|file|file.path==member));
        let positions=[args[5].parse::<usize>()?,args[6].parse::<usize>()?];
        let mut collector_rows=Vec::new();let mut base=0;
        for batch in ParquetRecordBatchReaderBuilder::try_new(File::open(&member)?)?.build()? {
            let batch=batch?;
            for position in positions {
                if position>=base && position<base+batch.num_rows() {
                    let row=polymarket_market_metadata_v6_row_from_batch(&batch,position-base)?;
                    collector_rows.push(serde_json::from_value::<data_collect::polymarket_market_metadata_layout::PolymarketMarketMetadataRow>(row.to_raw_value()?)?);
                }
            }
            base+=batch.num_rows();
        }
        assert_eq!(collector_rows.len(),2);
        data_collect::polymarket_market_metadata_layout::validate_polymarket_metadata_local_references(&collector_rows)?;
        let mut core=data_pipeline_core::public_observation_core::PublicObservationCoreV1::new(config.header.clone())?;
        let mut applied=0;
        for file in config.files.iter().filter(|file|file.path==member) {
            let mut base=0;
            for batch in data_pipeline_training::public_input::open_finite_public_input_v1(file,
                config.canonical.decode_batch_rows,config.canonical.max_decode_batch_bytes)? {
                for row in batch? {
                    if positions.contains(&base) {
                        assert_eq!(validate_metadata_initial_identity_v1(&row)?,applied==0);
                        let source=source_evidence_v3_from_typed_raw_v2(&row)?;
                        let record=RecordedIngressV1 {input_stream_id:config.header.input_stream_id.clone(),
                            input_position:applied,ingress:DptIngressV1::Message{row,source_evidence:source},
                            dependency_record_refs:Vec::new()};
                        let prepared=prepare_public_ingress_v1(&core,&record,&Default::default(),&[])?;
                        core.push(&prepared)?;applied+=1;
                    }
                    base+=1;
                }
            }
        }
        assert_eq!(applied,2);
        println!("{}",serde_json::json!({"original_collector_reference_rows":positions,
            "collector_reference_validated":true,"core_identity_and_successor_applied":applied}));
        return Ok(());
    }
    let mut config:CompactPublicPreparationConfigV1=serde_json::from_reader(File::open(&args[1])?)?;
    let output=PathBuf::from(&args[2]);
    let manifests=args[3..].iter().map(PathBuf::from).collect::<Vec<_>>();
    let started=Instant::now();let mut last=Instant::now();let mut files=0u64;let mut bytes=0u64;
    let mut notice=|event|if let HistoricalPreparationNoticeV1::AdmissionFile{bytes:n,..}=event {
        files+=1;bytes+=n;
        if last.elapsed()>=Duration::from_secs(10){
            eprintln!("{}",serde_json::json!({"stage":"manifest_admission","files":files,"bytes":bytes,"elapsed_seconds":started.elapsed().as_secs_f64()}));last=Instant::now();
        }
    };
    let notifier=HistoricalPreparationNotifierV1::new(&mut notice);
    let input=data_pipeline_training::freeze_public_source_input_set_with_progress_v1(&manifests,Some(&notifier))?;
    config.files.clear();
    for file in input.admitted_daily_inputs().iter().flat_map(|day|day.files()) {
        let hour=time::OffsetDateTime::parse(&format!("{}:00:00Z",file.utc_hour()),
            &time::format_description::well_known::Rfc3339)?.unix_timestamp()*1000;
        if hour<config.received_end_utc_ms && config.received_start_utc_ms<hour+3_600_000 {
            config.files.push(data_pipeline_training::public_input::PublicFiniteFileV1{
                path:file.file_path().to_path_buf(),source_kind:file.source_kind().into(),schema_version:file.schema_version().into(),
                bytes:file.bytes(),rows:file.rows(),sha256:file.sha256().into(),
            });
        }
    }
    serde_json::to_writer(File::create_new(&output)?,&config)?;
    println!("{}",serde_json::json!({"input_set_identity":input.input_set_identity_sha256(),
        "input_manifest_set":input.input_manifest_set_sha256(),"admitted_files":files,"admitted_bytes":bytes,
        "selected_files":config.files.len(),"selected_rows":config.files.iter().map(|f|f.rows).sum::<u64>(),
        "selected_bytes":config.files.iter().map(|f|f.bytes).sum::<u64>(),"elapsed_seconds":started.elapsed().as_secs_f64()}));
    Ok(())
}
