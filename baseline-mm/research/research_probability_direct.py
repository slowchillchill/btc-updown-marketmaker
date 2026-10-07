#!/usr/bin/env python3
"""直接概率校准入口，不采用正式生产参数。"""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'data-pipeline/training/python'))
from probability.cli import main
if __name__=='__main__':main()
