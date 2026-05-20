"""
pipeline.py — End-to-end runner for the VendorPay data pipeline.

Usage:
    python pipeline.py                 # run all stages
    python pipeline.py ingest          # run ingestion only
    python pipeline.py transform       # run transformation only
    python pipeline.py features        # run feature engineering only
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Ensure src/ is importable when running from the project root
sys.path.insert(0, str(Path(__file__).parent / "src"))

import ingest
import transform
import features


STAGES = {
    "ingest": ingest.run,
    "transform": transform.run,
    "features": features.run,
}


def run_all() -> None:
    for stage, fn in STAGES.items():
        log.info("=== Stage: %s ===", stage)
        fn()
    log.info("=== Pipeline complete ===")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        run_all()
    else:
        for stage in args:
            if stage not in STAGES:
                log.error("Unknown stage '%s'. Choose from: %s", stage, list(STAGES))
                sys.exit(1)
            log.info("=== Stage: %s ===", stage)
            STAGES[stage]()
