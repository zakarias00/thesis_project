"""
Runs the pipeline to reproduce all outputs

Usage:
    python -m ecsf_pipeline_pkg.runner [--config pipeline_config.json]
    python run_pipeline.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="ECSF Cybersecurity Education Analytics Pipeline"
    )
    args = parser.parse_args()

    from thesis_project.ecsf_pipeline_pkg.config import PipelineConfig
    from thesis_project.ecsf_pipeline_pkg.orchestrator import AnalyticsPipeline

    if args.config and Path(args.config).exists():
        config = PipelineConfig.load(args.config)
    else:
        config = PipelineConfig()

    config.output_dir = args.output_dir

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
