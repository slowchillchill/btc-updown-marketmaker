#!/usr/bin/env python3
"""Repository entry point; the public projection stays in data-pipeline."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"data-pipeline/training/python"))
from source_health.cli import main
if __name__=="__main__":
    main()
