#!/usr/bin/env python3
"""复用既有归档前台监督器，每阶段仅执行一次。"""
from pathlib import Path
from run_source_health_columnar_once import main
if __name__=='__main__':
    raise SystemExit(main(Path(__file__).with_name('research_probability_direct.py')))
