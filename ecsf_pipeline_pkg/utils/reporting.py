from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from ..config import PipelineConfig

logger = logging.getLogger("ecsf_pipeline.reporting")


# Write pipeline results as CSV or JSON
class ReportExporter:

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._out = Path(config.output_dir)
        self._out.mkdir(parents=True, exist_ok=True)

    def export_scores(self, scores_df: pd.DataFrame) -> str:
        path = str(self._out / "scores.csv")
        scores_df.to_csv(path, index=False)
        logger.info("Wrote %s", path)
        return path

    def export_soa_flat(self, soa_df: pd.DataFrame) -> str:
        path = str(self._out / "soa_flat.csv")
        soa_df.to_csv(path, index=False)
        logger.info("Wrote %s", path)
        return path

    def export_recommendations(self, recommendations: list[dict]) -> str:
        path = str(self._out / "recommendations.json")
        with open(path, "w") as f:
            json.dump(recommendations, f, indent=2, default=str)
        logger.info("Wrote %s", path)
        return path

    def export_validation(self, validation_summary: dict) -> str:
        path = str(self._out / "validation_summary.json")
        with open(path, "w") as f:
            json.dump(validation_summary, f, indent=2, default=str)
        logger.info("Wrote %s", path)
        return path

    def export_pipeline_summary(self, metadata: dict) -> str:
        """Write pipeline_summary.json with run metadata."""
        path = str(self._out / "pipeline_summary.json")
        with open(path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)
        logger.info("Wrote %s", path)
        return path

    def export_pipeline_report(
        self,
        metadata: dict,
        scores: list[dict],
        validation_summary: dict | None = None,
    ) -> str:
        """Write a human-readable pipeline_report.md."""
        path = str(self._out / "pipeline_report.md")
        lines = [
            "# ECSF Cybersecurity Education Pipeline Report",
            "",
            f"**Generated:** {metadata.get('timestamp', 'N/A')}",
            "",
            f"**Pipeline version:** 2.0.0",
            "",
            "## Pipeline Summary",
            "",
            f"- **Elapsed:** {metadata.get('elapsed_seconds', 0)} s",
            f"- **Programs analyzed:** {metadata.get('n_programs', 0)}",
            f"- **Framework items:** {metadata.get('n_framework_items', 0)}",
            f"- **SOA entries:** {metadata.get('total_soa_entries', 0)}",
            f"- **Stages executed:** {', '.join(metadata.get('stages_executed', []))}",
            "",
        ]

        # Stages checklist 
        lines.append("## Stages Executed")
        lines.append("")
        executed = set(metadata.get("stages_executed", []))
        all_possible = [
            "ingestion", "framework_mapping", "lo_extraction",
            "assessment_extraction", "soa", "breadth", "depth",
            "progression", "immersion", "nlp", "ontology",
            "embeddings", "scoring", "validation", "feedback",
            "graph", "reporting",
        ]
        for stage in all_possible:
            mark = "x" if stage in executed else " "
            lines.append(f"- [{mark}] {stage}")
        lines.append("")

        # Top programs table
        if scores:
            lines.append("## Top Programs by Composite Score")
            lines.append("")
            lines.append("| Rank | Program | University | Score | Grade |")
            lines.append("|------|---------|-----------|-------|-------|")
            sorted_scores = sorted(
                scores, key=lambda s: s.get("composite_score", 0), reverse=True
            )
            for rank, s in enumerate(sorted_scores[:20], 1):
                lines.append(
                    f"| {rank} | {s.get('program', '')} "
                    f"| {s.get('university', '')} "
                    f"| {s.get('composite_score', 0):.3f} "
                    f"| {s.get('grade', '')} |"
                )
            lines.append("")

            # Score distribution 
            comp_scores = [s.get("composite_score", 0) for s in scores]
            import numpy as _np
            arr = _np.array(comp_scores)
            lines.append("## Score Distribution")
            lines.append("")
            lines.append(f"- **mean:** {arr.mean():.4f}")
            lines.append(f"- **std:** {arr.std():.4f}")
            lines.append(f"- **min:** {arr.min():.4f}")
            lines.append(f"- **25%:** {_np.percentile(arr, 25):.4f}")
            lines.append(f"- **50%:** {_np.percentile(arr, 50):.4f}")
            lines.append(f"- **75%:** {_np.percentile(arr, 75):.4f}")
            lines.append(f"- **max:** {arr.max():.4f}")
            lines.append("")

        # Validation summary
        if validation_summary:
            lines.append("## Validation Summary")
            lines.append("")
            ci = validation_summary.get("coverage_improvement_regex_to_full", 0)
            lines.append(f"- **Coverage improvement (regex → full):** +{ci:.4f}")
            rc = validation_summary.get("rank_correlation", {})
            lines.append(f"- **Spearman ρ:** {rc.get('spearman_rho', 0):.4f}")
            ts = validation_summary.get("top_n_stability", {})
            lines.append(f"- **Top-N stability:** {ts.get('average_stability', 0)}")
            lines.append("")

            # Ablation table
            ablation = validation_summary.get("ablation_summary", {})
            if ablation:
                lines.append("### Ablation Results")
                lines.append("")
                lines.append("| Method | Mean Coverage | Std |")
                lines.append("|--------|--------------|-----|")
                for method in ["regex_only", "regex_nlp", "regex_nlp_ontology", "full"]:
                    entry = ablation.get(method, {})
                    lines.append(
                        f"| {method} | {entry.get('mean', 0):.4f} | {entry.get('std', 0):.4f} |"
                    )
                lines.append("")

            # Method comparison table
            mc = validation_summary.get("method_comparison", [])
            if mc:
                lines.append("### Framework Method Comparison (Jaccard)")
                lines.append("")
                lines.append("| Method A | Method B | Mean Jaccard | Median | Std |")
                lines.append("|----------|----------|-------------|--------|-----|")
                for row in mc:
                    lines.append(
                        f"| {row['method_a']} | {row['method_b']} "
                        f"| {row['mean_jaccard']:.4f} | {row['median_jaccard']:.4f} "
                        f"| {row['std_jaccard']:.4f} |"
                    )
                lines.append("")

        lines.append("---")
        with open(path, "w") as f:
            f.write("\n".join(lines))
        logger.info("Wrote %s", path)
        return path

    def export_artifact_manifest(self, metadata: dict, row_counts: dict) -> str:
        """Write artifact_manifest.json cataloguing pipeline outputs."""
        path = str(self._out / "artifact_manifest.json")
        artifacts = [f.name for f in self._out.iterdir() if f.is_file()]
        manifest = {
            "stages_executed": metadata.get("stages_executed", []),
            "row_counts": row_counts,
            "timestamp": metadata.get("timestamp", ""),
            "elapsed_seconds": metadata.get("elapsed_seconds", 0),
            "artifacts": sorted(artifacts),
        }
        with open(path, "w") as f:
            json.dump(manifest, f, indent=2, default=str)
        logger.info("Wrote %s", path)
        return path
