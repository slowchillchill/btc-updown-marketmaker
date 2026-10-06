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
from . import intervals, statistics, direct_study


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
    db=duckdb.connect(':memory:')
    db.execute("SET threads=?",[engine["threads"]])
    db.execute("SET memory_limit=?",[engine["memory_limit"]])
    # Identity and causal order use explicit native/file-row coordinates. Keeping
    # physical insertion order adds import buffering without a business consumer.
    db.execute("SET preserve_insertion_order=false")
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
    try:
        helper=Path(config["boundary_helper"])
        init_config=work/"initialization.private.json";save(init_config,config["metadata_initialization"])
        initialization=material/"initialization.jsonl"
        subprocess.run([str(helper),"initialization",str(init_config),str(initialization)],check=True)
        rules=json.loads(subprocess.check_output([str(helper),"rules"],text=True))
        progress("direct-file-index",total_raw_rows=sum(f["rows"] for f in config["files"]))
        direct_study.metadata(db,config,initialization,rules)
        direct_study.setup(db,config,material)
        ranges,units=direct_study.query_plan(db,config,progress)
        from dataclasses import asdict
        save(material/"query-units.json",[asdict(u) for u in units])
        save(material/"file-ranges.json",[asdict(r) for r in ranges])
        save(material/"epoch-ordering.json",direct_study.ordering_summary(ranges,units))
        from dpt_health.projection import literal
        for table in ("markets","rules","selections"):
            db.execute(f"COPY {table} TO {literal(material/(table+'.parquet'))} (FORMAT PARQUET,COMPRESSION ZSTD)")
        direct_study.distributions(db,config,material,work,units,progress,ranges)
        summary=dict(engine_versions=dict(duckdb=duckdb.__version__),format="direct-parquet-health-bins-v2",start=config["start"],end=config["end"],
                     binance_trade_enabled=config["binance_trade_enabled"],binance_depth_enabled=config["binance_depth_enabled"],
                     config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
                     raw_rows=sum(f["rows"] for f in config["files"]),files=len(config["files"]),units=len(units),
                     maximum_candidate_file_rows=max((u.candidate_file_rows for u in units),default=0),
                     clocks="direct received/event; whole-PM/source-native frontier; condition-local sync; no legacy global replay clock",
                     outputs=[dict(path=p.name,bytes=p.stat().st_size) for p in sorted(material.iterdir())])
        save(material/"material.json",summary)
        progress("prepare-complete")
    finally:
        progress.close();db.close()


def study(config_path):
    config=json.loads(config_path.read_text());root=Path(config["run_root"])
    material=root/"material";summary=json.loads((material/"material.json").read_text())
    if summary["format"]!="direct-parquet-health-bins-v2" or summary["config_sha256"]!=hashlib.sha256(config_path.read_bytes()).hexdigest():
        raise ValueError("incompatible material")
    output=root/"study";output.mkdir(exist_ok=False)
    work=root/"statistics"
    db=connection(work,config["engine"])
    progress=Progress(db,config["progress_interval_seconds"],work)
    try:
        progress("direct-bin-load")
        direct_study.setup(db,config,material)
        from dpt_health.direct import QueryUnit
        units=[QueryUnit(**dict(u,file_ids=tuple(u["file_ids"]))) for u in json.loads((material/"query-units.json").read_text())]
        from dpt_health.projection import literal
        db.execute(f"CREATE VIEW selected_histogram AS SELECT * FROM read_parquet({literal(material/'selected_histogram.parquet')})")
        start=config["start"];day=intervals.DAY
        for fold,fit_days in enumerate([3,6,9,12,15]):
            cutoff=start+fit_days*day
            progress("forward-fit",fold=fold,fit_end=cutoff)
            candidates=statistics.fit_candidates(db,cutoff,Path(config["statistics_helper"]),work)
            save(output/f"fold-{fold}.frozen.json",dict(fold=fold,fit=[start,cutoff],candidates=candidates))
            progress("frozen-validation",fold=fold)
            validation=direct_study.validate_candidates(db,config,material,output,work,units,candidates,cutoff,cutoff+3*day,fold,progress)
            save(output/f"fold-{fold}.validation.json",dict(fold=fold,window=[cutoff,cutoff+3*day],development=fold==4,results=validation))
        save(output/"summary.json",dict(material=summary,engine=config["engine"],method="daily body p90 after Q3+1.5IQR; lower median across fit days; 2/4/8 hypotheses",formal_adoption=False))
        progress("study-complete")
    finally:
        progress.close();db.close()


def main():
    parser=argparse.ArgumentParser(description="Direct Parquet SQL for daily source-health bins and frozen validation. No trading parameter adoption.")
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
