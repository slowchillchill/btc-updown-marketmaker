"""Columnar research command. A run never adopts production parameters."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import resource
import subprocess
import threading
import time
from pathlib import Path
import duckdb
from dpt_health import projection
from . import intervals, statistics


def save(path, value):
    with path.open("x") as output:
        json.dump(value,output,ensure_ascii=False,indent=2)
        output.write("\n")


class Progress:
    """One 30-second observer; native query progress plus actual process counters."""
    def __init__(self, db, seconds, work):
        self.work=work
        self.db=db; self.seconds=seconds; self.started=time.monotonic()
        self.phase="starting"; self.raw_rows=0; self.files=0; self.detail={}
        self.stop=threading.Event(); self.lock=threading.Lock()
        self.thread=threading.Thread(target=self.observe,daemon=False)
        self.thread.start()

    def __call__(self, phase, raw_rows=0, files=0, **detail):
        with self.lock:
            changed=self.phase!=phase
            self.phase=phase; self.raw_rows+=raw_rows; self.files+=files; self.detail=detail
        if changed:
            self.emit()

    def emit(self):
        usage=resource.getrusage(resource.RUSAGE_SELF)
        rss=None
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss=int(line.split()[1])*1024
        with self.lock:
            row=dict(phase=self.phase,raw_rows=self.raw_rows,files=self.files,**self.detail)
        row.update(elapsed_s=round(time.monotonic()-self.started,3),rss_bytes=rss,
                   peak_rss_bytes=usage.ru_maxrss*1024,cpu_user_s=usage.ru_utime,cpu_system_s=usage.ru_stime)
        # DuckDB's API is an engine progress observation, not another SQL query.
        row["engine_query_progress"]=self.db.query_progress()
        row["filesystem_available_bytes"]=os.statvfs(self.work).f_bavail*os.statvfs(self.work).f_frsize
        database=self.work/"working.duckdb"
        row["working_database_allocated_bytes"]=database.stat().st_blocks*512 if database.exists() else 0
        spill=self.work/"spill";allocated=0
        if spill.exists():
            for entry in os.scandir(spill):
                try:
                    if entry.is_file():
                        allocated+=entry.stat().st_blocks*512
                except FileNotFoundError:
                    pass  # A spill block can be released between directory and stat reads.
        row["spill_allocated_bytes"]=allocated
        row["io"]={line.split(':')[0]:int(line.split(':')[1]) for line in Path("/proc/self/io").read_text().splitlines()}
        print(json.dumps(row,ensure_ascii=False),flush=True)

    def observe(self):
        while not self.stop.wait(self.seconds):
            self.emit()

    def close(self):
        self.stop.set();self.thread.join()


def connection(root, engine):
    root.mkdir(exist_ok=False)
    db=duckdb.connect(str(root/"working.duckdb"))
    db.execute("SET threads=?",[engine["threads"]])
    db.execute("SET memory_limit=?",[engine["memory_limit"]])
    db.execute("SET temp_directory=?",[str(root/"spill")])
    db.execute("SET max_temp_directory_size=?",[engine["max_temp_directory_size"]])
    db.execute("SET enable_progress_bar=true")
    db.execute("SET enable_progress_bar_print=false")
    return db


def configure(plan_path, input_root, run_root, helper, statistics_helper, output):
    plan=json.loads(plan_path.read_text())
    files=[]
    for expected in plan["manifests"]:
        manifest_path=input_root/expected["path"]
        payload=manifest_path.read_bytes()
        if hashlib.sha256(payload).hexdigest()!=expected["sha256"]:
            raise ValueError("manifest differs from original admitted identity")
        manifest=json.loads(payload)
        # Match the original configure: every file in each same ordered manifest.
        for file in manifest["files"]:
            path=(manifest_path.parent/file["path"]).resolve()
            if not path.is_relative_to(input_root.resolve()):
                raise ValueError("manifest file escapes explicit input root")
            files.append(dict(file,path=str(path)))
    initialization=plan["metadata_initialization"]
    for file in initialization["files"]:
        file["manifest_path"]=str(input_root/file["manifest_path"])
    config=dict(plan,files=files,metadata_initialization=initialization,
                run_root=str(run_root),boundary_helper=str(helper.resolve()),statistics_helper=str(statistics_helper.resolve()))
    config.pop("manifests")
    save(output,config)


def prepare(config_path):
    config=json.loads(config_path.read_text())
    root=Path(config["run_root"]); root.mkdir(exist_ok=False)
    material=root/"material";material.mkdir()
    work=root/"projection"
    db=connection(work,config["engine"])
    progress=Progress(db,config["progress_interval_seconds"],work)
    completed=False
    try:
        helper=Path(config["boundary_helper"])
        init_config=work/"initialization.private.json";save(init_config,config["metadata_initialization"])
        initialization=material/"initialization.jsonl"
        subprocess.run([str(helper),"initialization",str(init_config),str(initialization)],check=True)
        rules=json.loads(subprocess.check_output([str(helper),"rules"],text=True))
        progress("necessary-columns-start",total_raw_rows=sum(f["rows"] for f in config["files"]))
        kinds=projection.read_columns(db,config["files"],progress)
        progress("market-identity-and-rules")
        projection.projected_markets(db,initialization,rules)
        pm=projection.deduplicate(db,"raw_polymarket_market",["market","source_sequence_epoch_id","collector_admission_sequence"],helper,config["files"],work/"duplicates-pm")
        progress("pm-minimal-state")
        projection.make_pm_events(db,pm)
        projection.make_common_events(db,kinds,helper,config["files"],work,progress)
        progress("persist-narrow-facts")
        projection.state_changes(db,material)
        summary=dict(engine_versions=dict(duckdb=duckdb.__version__),format="columnar-source-facts-v1",start=config["start"],end=config["end"],
                     binance_trade_enabled=config["binance_trade_enabled"],binance_depth_enabled=config["binance_depth_enabled"],
                     config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
                     raw_rows=sum(f["rows"] for f in config["files"]),files=len(config["files"]),
                     clocks="direct last-valid received/event; admitted and source-local/dependency receive frontier; no legacy global replay clock",
                     outputs=[dict(path=p.name,bytes=p.stat().st_size) for p in sorted(material.iterdir())])
        save(material/"material.json",summary)
        progress("prepare-complete")
        completed=True
    finally:
        progress.close();db.close()
        if completed:
            (work/"working.duckdb").unlink()


def study(config_path):
    config=json.loads(config_path.read_text());root=Path(config["run_root"])
    material=root/"material";summary=json.loads((material/"material.json").read_text())
    if summary["format"]!="columnar-source-facts-v1" or summary["config_sha256"]!=hashlib.sha256(config_path.read_bytes()).hexdigest():
        raise ValueError("incompatible material")
    output=root/"study";output.mkdir(exist_ok=False)
    work=root/"statistics"
    db=connection(work,config["engine"])
    progress=Progress(db,config["progress_interval_seconds"],work)
    completed=False
    try:
        progress("continuous-intervals")
        intervals.load_intervals(db,material,config["start"],config["end"],config["binance_trade_enabled"],config["binance_depth_enabled"])
        statistics.histogram_macros(db)
        intervals.daily_intervals(db,"selected_intervals","selected_daily")
        intervals.daily_intervals(db,"intervals","source_daily")
        progress("selected-duration-histograms")
        statistics.histogram(db,"selected_daily","selected_histogram")
        progress("source-duration-histograms")
        statistics.histogram(db,"source_daily","source_histogram",grouping="source")
        from dpt_health.projection import literal
        for table in ("selected_histogram","source_histogram"):
            db.execute(f"COPY {table} TO {literal(output/(table+'.parquet'))} (FORMAT PARQUET,COMPRESSION ZSTD)")
        # Independent exclusions and unknown sync/generation do not disappear
        # into age quantiles. Exact source/asset/day denominators remain visible.
        db.execute(f"""COPY (SELECT source,condition_id,asset,day_start,exclusion,
            sum(db-da)::HUGEINT AS duration_ms,sum(CASE WHEN sync_unknown THEN db-da ELSE 0 END)::HUGEINT AS sync_unknown_ms,
            sum(CASE WHEN generation_unknown THEN db-da ELSE 0 END)::HUGEINT AS generation_unknown_ms
            FROM source_daily GROUP BY source,condition_id,asset,day_start,exclusion)
            TO {literal(output/'source-denominators.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)""")
        db.execute(f"""COPY (SELECT a.source,a.condition_id,a.asset,(a.visible_at//{intervals.DAY})*{intervals.DAY} AS day_start,
            CASE WHEN a.sync_required THEN 'synchronization_required' WHEN NOT a.available THEN 'unavailable' WHEN a.event IS NULL THEN 'unknown_event_time' WHEN a.event>a.received THEN 'time_inconsistent' ELSE 'valid' END AS status,
            CASE WHEN NOT a.sync_required AND a.available AND a.event<=a.received THEN age_bin(a.received-a.event) END AS arrival_bin,count(*) AS samples,
            sum((a.source='polymarket' OR EXISTS(SELECT 1 FROM selections z WHERE z.source=a.source AND a.visible_at>=z.a AND a.visible_at<z.b))::INTEGER) AS selected_samples
            FROM arrivals a JOIN scopes s USING(source,condition_id,asset) WHERE a.visible_at>=s.a AND a.visible_at<s.b
            GROUP BY a.source,a.condition_id,a.asset,day_start,status,arrival_bin)
            TO {literal(output/'arrival-samples.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)""")
        start=config["start"];day=intervals.DAY
        for fold,fit_days in enumerate([3,6,9,12,15]):
            cutoff=start+fit_days*day
            progress("forward-fit",fold=fold,fit_end=cutoff)
            candidates=statistics.fit_candidates(db,cutoff,Path(config["statistics_helper"]),work)
            save(output/f"fold-{fold}.frozen.json",dict(fold=fold,fit=[start,cutoff],candidates=candidates))
            progress("frozen-validation",fold=fold)
            validation=[]
            for candidate in candidates:
                name=f"fold-{fold}.{candidate['identifier']}"
                validation.append(statistics.evaluate(db,candidate,cutoff,cutoff+3*day,name,output))
            save(output/f"fold-{fold}.validation.json",dict(fold=fold,window=[cutoff,cutoff+3*day],development=fold==4,results=validation))
        save(output/"summary.json",dict(material=summary,engine=config["engine"],method="daily body p90 after Q3+1.5IQR; lower median across fit days; 2/4/8 hypotheses",formal_adoption=False))
        progress("study-complete")
        completed=True
    finally:
        progress.close();db.close()
        if completed:
            (root/"statistics/working.duckdb").unlink()


def main():
    parser=argparse.ArgumentParser(description="Necessary-column source facts and frozen source-health research. No trading parameter adoption.")
    sub=parser.add_subparsers(dest="command",required=True)
    configure_parser=sub.add_parser("configure",help="bind original manifest identities without reading raw rows")
    for name in ("plan","input_root","run_root","boundary_helper","statistics_helper","output"):
        configure_parser.add_argument(name,type=Path)
    for command in ("prepare","study"):
        sub.add_parser(command).add_argument("config",type=Path)
    args=parser.parse_args()
    if args.command=="configure":
        configure(args.plan,args.input_root,args.run_root,args.boundary_helper,args.statistics_helper,args.output)
    elif args.command=="prepare":
        prepare(args.config)
    else:
        study(args.config)


if __name__=="__main__":
    main()
