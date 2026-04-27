"""
Runs the pipeline to reproduce all outputs

Usage:
    python -m ecsf_pipeline_pkg.runner
    python run_pipeline.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from ecsf_pipeline_pkg.config import PipelineConfig
from ecsf_pipeline_pkg.orchestrator import AnalyticsPipeline


def main():
    config = PipelineConfig()

    pipeline = AnalyticsPipeline(config)
    summary = pipeline.run()

    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"\nOutputs: {config.output_dir}/")


if __name__ == "__main__":
    main()
