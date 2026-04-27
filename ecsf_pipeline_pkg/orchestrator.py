from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .schemas import (
    standardize_course_df,
    FrameworkItem,
    build_framework_items_ecsf,
    build_framework_items_nice,
    build_framework_items_jrc,
)

# Stage imports
from .stages.ingestion import DataIngestion
from .stages.extraction import LearningOutcomeExtractor, AssessmentMethodExtractor
from .stages.framework_mapping import (
    ECSFRoleMatcher, NICERoleMatcher, JRCTaxonomyMatcher, CrossFrameworkAligner,
)
from .stages.soa import SkillAligner
from .stages.analysis import (
    CoverageBreadthAnalyzer, CoverageDepthAnalyzer,
    ProgressionCoherenceAnalyzer, PracticalImmersionAnalyzer,
)
from .stages.semantic import NLPCompetencyExtractor, OntologyAligner, EmbeddingAnalyzer
from .stages.feedback import CurriculumFeedback

# Scoring imports
from .scoring.quality_scores import (
    CoverageScoreCalculator, RedundancyScoreCalculator,
    SkillDepthScoreCalculator, UpdateLatencyScoreCalculator,
    CompositeQualityScoreCalculator,
)
from .scoring.validation import ValidationSuite

# Graph imports
from .graph.graph_pipeline import GraphPipeline, export_networkx_graphml

# Reporting imports
from .utils.reporting import ReportExporter
logger = logging.getLogger("ecsf_pipeline")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


class AnalyticsPipeline:
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self._out = Path(self.config.output_dir)
        self._out.mkdir(parents=True, exist_ok=True)

        # ── Ingestion ──
        self.ingestion = DataIngestion(self.config)

        # ── Extraction ──
        self.lo_extractor = LearningOutcomeExtractor(self.config)
        self.am_extractor = AssessmentMethodExtractor(self.config)

        # ── Framework matchers ──
        self.ecsf_matcher = ECSFRoleMatcher(self.config)
        self.nice_matcher = NICERoleMatcher(self.config)
        self.jrc_matcher = JRCTaxonomyMatcher(self.config)

        # ── SOA ──
        self.aligner = SkillAligner(self.config)

        # ── Analysis ──
        self.breadth_analyzer = CoverageBreadthAnalyzer(self.config)
        self.depth_analyzer = CoverageDepthAnalyzer(self.config)
        self.progression_analyzer = ProgressionCoherenceAnalyzer(self.config)
        self.immersion_analyzer = PracticalImmersionAnalyzer(self.config)

        # ── Semantic methods (lazy — heavy models loaded only when enabled) ──
        self._nlp_extractor: NLPCompetencyExtractor | None = None
        self._ontology_aligner: OntologyAligner | None = None
        self._embedding_analyzer: EmbeddingAnalyzer | None = None

        # ── Scoring ──
        self.coverage_scorer = CoverageScoreCalculator(self.config)
        self.redundancy_scorer = RedundancyScoreCalculator(self.config)
        self.depth_scorer = SkillDepthScoreCalculator(self.config)
        self.latency_scorer = UpdateLatencyScoreCalculator(self.config)
        self.composite_scorer = CompositeQualityScoreCalculator(self.config)

        # ── Validation ──
        self.validator = ValidationSuite(self.config)

        # ── Feedback ──
        self.feedback = CurriculumFeedback(self.config)

        # ── Reporting ──
        self.exporter = ReportExporter(self.config)

        # ── State ──
        self.courses_df: pd.DataFrame | None = None
        self.enisa_df: pd.DataFrame | None = None
        self.nice_df: pd.DataFrame | None = None
        self.taxonomy_df: pd.DataFrame | None = None

        self.framework_items: list[FrameworkItem] = []
        self.soa_results: list[dict] = []
        self.all_scores: list[dict] = []
        self.recommendations: list[dict] = []
        self.validation_summary: dict = {}
        self._run_metadata: dict = {}

    # Execute the full pipeline.  Returns metadata summary
    def run(self) -> dict:
        start = datetime.now()
        logger.info("Pipeline start")
        cfg = self.config
        stages_executed: list[str] = []

        stages_executed.append("ingestion")
        self.courses_df = self.ingestion.ingest_courses()
        self.enisa_df = self.ingestion.ingest_enisa()

        if cfg.enable_nice:
            self.nice_df = self.ingestion.ingest_nice()
        if cfg.enable_jrc:
            self.taxonomy_df = self.ingestion.ingest_jrc_taxonomy()

        # Determine description column
        desc_col = "description" if "description" in self.courses_df.columns else "study program description"

        
        if cfg.run_framework_mapping:
            logger.info("Stage: framework mapping")
            stages_executed.append("framework_mapping")
            self.ecsf_matcher.build_index(self.enisa_df)
            self.ecsf_matcher.batch_match(self.courses_df, desc_col)

            if cfg.enable_nice and self.nice_df is not None:
                self.nice_matcher.build_index(self.nice_df)
                self.nice_matcher.batch_match(self.courses_df, desc_col)

            if cfg.enable_jrc and self.taxonomy_df is not None:
                self.jrc_matcher.build_index(self.taxonomy_df)
                self.jrc_matcher.batch_match(self.courses_df, desc_col)

            CrossFrameworkAligner.summarize(self.courses_df)

        
        self.framework_items = build_framework_items_ecsf(self.enisa_df)
        if cfg.enable_nice and self.nice_df is not None:
            self.framework_items += build_framework_items_nice(self.nice_df)
        if cfg.enable_jrc and self.taxonomy_df is not None:
            self.framework_items += build_framework_items_jrc(self.taxonomy_df)

        
        if cfg.run_lo_extraction:
            logger.info("Stage: LO extraction")
            stages_executed.append("lo_extraction")
            self.courses_df["learning_outcomes"] = self.courses_df[desc_col].apply(
                self.lo_extractor.extract_outcomes
            )
            self.courses_df["n_outcomes"] = self.courses_df["learning_outcomes"].apply(len)

        if cfg.run_assessment_extraction:
            logger.info("Stage: assessment extraction")
            stages_executed.append("assessment_extraction")
            self.courses_df["assessment_methods"] = self.courses_df[desc_col].apply(
                self.am_extractor.extract_methods
            )
            self.courses_df["n_assessments"] = self.courses_df["assessment_methods"].apply(len)

        
        if cfg.run_soa:
            logger.info("Stage: SOA matrix construction")
            stages_executed.append("soa")
            self.soa_results = []
            for _, row in self.courses_df.iterrows():
                outcomes = row.get("learning_outcomes", [])
                assessments = row.get("assessment_methods", [])
                soa = self.aligner.build_soa_matrix(
                    row, self.framework_items,
                    outcomes if isinstance(outcomes, list) else [],
                    assessments if isinstance(assessments, list) else [],
                )
                self.soa_results.append(soa)

            # Export flat SOA
            soa_flat = SkillAligner.flatten_all_soa(self.soa_results)
            self.exporter.export_soa_flat(soa_flat)

        # Breadth / depth / progression / immersion
        if cfg.run_breadth:
            logger.info("Stage: coverage breadth")
            stages_executed.append("breadth")
            self.courses_df["breadth_result"] = self.courses_df[desc_col].apply(
                lambda d: self.breadth_analyzer.analyze_program(d, self.enisa_df)
            )
            self.courses_df["overall_breadth"] = self.courses_df["breadth_result"].apply(
                lambda r: r["overall_breadth"]
            )

        if cfg.run_depth:
            logger.info("Stage: coverage depth")
            stages_executed.append("depth")
            self.courses_df["depth_result"] = self.courses_df.apply(
                lambda row: self.depth_analyzer.analyze_program(
                    row[desc_col],
                    self.enisa_df,
                    row.get("ecsf_roles", []) or [],
                ),
                axis=1,
            )
            self.courses_df["overall_depth"] = self.courses_df["depth_result"].apply(
                lambda r: r["overall_depth"]
            )

        if cfg.run_progression:
            logger.info("Stage: progression coherence")
            stages_executed.append("progression")
            self.courses_df["progression_result"] = self.courses_df[desc_col].apply(
                self.progression_analyzer.analyze_program
            )
            self.courses_df["coherence_score"] = self.courses_df["progression_result"].apply(
                lambda r: r["coherence_score"]
            )

        if cfg.run_immersion:
            logger.info("Stage: practical immersion")
            stages_executed.append("immersion")
            self.courses_df["immersion_result"] = self.courses_df[desc_col].apply(
                self.immersion_analyzer.analyze_program
            )
            self.courses_df["immersion_index"] = self.courses_df["immersion_result"].apply(
                lambda r: r["immersion_index"]
            )

        if cfg.run_nlp:
            logger.info("Stage: NLP competency extraction")
            try:
                self._nlp_extractor = NLPCompetencyExtractor(self.config)
                self._nlp_extractor.build_vocabulary(self.enisa_df)
                self.courses_df["nlp_result"] = self.courses_df[desc_col].apply(
                    self._nlp_extractor.extract_competencies
                )
                self.courses_df["nlp_role_scores"] = self.courses_df["nlp_result"].apply(
                    lambda r: r.get("nlp_role_scores", {})
                )
                stages_executed.append("nlp")
            except (ImportError, ModuleNotFoundError, OSError) as e:
                logger.warning("NLP stage skipped – dependency unavailable: %s", e)

        if cfg.run_ontology:
            logger.info("Stage: ontology alignment")
            try:
                self._ontology_aligner = OntologyAligner(self.config)
                self._ontology_aligner.build_ontology(self.enisa_df)
                ontology_path = str(self._out / "ecsf_ontology.ttl")
                self._ontology_aligner.export_ontology(ontology_path)

                if self._nlp_extractor:
                    self.courses_df["ontology_result"] = self.courses_df[desc_col].apply(
                        lambda d: self._ontology_aligner.align_description(d, self._nlp_extractor)
                    )
                    self.courses_df["ontology_boosted_scores"] = self.courses_df["ontology_result"].apply(
                        lambda r: r.get("graph_boosted", {})
                    )
                stages_executed.append("ontology")
            except (ImportError, ModuleNotFoundError, OSError) as e:
                logger.warning("Ontology stage skipped – dependency unavailable: %s", e)
                
        if cfg.run_embeddings:
            logger.info("Stage: embedding analysis")
            try:
                self._embedding_analyzer = EmbeddingAnalyzer(self.config)
                self._embedding_analyzer.build_framework_index(self.enisa_df)

                self.courses_df["embedding_result"] = self.courses_df[desc_col].apply(
                    self._embedding_analyzer.compute_similarity_matrix
                )
                self.courses_df["embedding_role_scores"] = self.courses_df["embedding_result"].apply(
                    lambda r: r.get("role_scores", {})
                )
                self.courses_df["embedding_mean_score"] = self.courses_df["embedding_result"].apply(
                    lambda r: r.get("mean_score", 0.0)
                )

                # Gap detection
                gaps = self._embedding_analyzer.detect_semantic_gaps(self.courses_df)
                gap_path = str(self._out / "semantic_gap_items.csv")
                if gaps.get("gap_items"):
                    pd.DataFrame(gaps["gap_items"]).to_csv(gap_path, index=False)

                # Clustering
                clusters = self._embedding_analyzer.cluster_programs(self.courses_df)
                self.courses_df["cluster_label"] = clusters["labels"]
                cluster_path = str(self._out / "program_clusters.csv")
                name_col = "study_program_name" if "study_program_name" in self.courses_df.columns else "study program name"
                uni_col = "university_name" if "university_name" in self.courses_df.columns else "university name"
                self.courses_df[[name_col, uni_col, "cluster_label"]].to_csv(cluster_path, index=False)
                stages_executed.append("embeddings")
            except (ImportError, ModuleNotFoundError, OSError) as e:
                logger.warning("Embedding stage skipped – dependency unavailable: %s", e)

        if cfg.run_scoring:
            logger.info("Stage: quality scoring")
            stages_executed.append("scoring")
            self.all_scores = []

            all_coverage_details: list[list[dict]] = []
            program_names: list[str] = []
            name_col = "study_program_name" if "study_program_name" in self.courses_df.columns else "study program name"
            uni_col = "university_name" if "university_name" in self.courses_df.columns else "university name"

            for idx, (_, row) in enumerate(self.courses_df.iterrows()):
                desc = row[desc_col]
                nlp_scores = row.get("nlp_role_scores", {}) or {}
                emb_scores = row.get("embedding_role_scores", {}) or {}
                onto_scores = row.get("ontology_boosted_scores", {}) or {}

                # Coverage
                cov = self.coverage_scorer.score_program(
                    desc, self.framework_items, nlp_scores, emb_scores, onto_scores
                )
                all_coverage_details.append(cov["per_item_detail"])
                program_names.append(row[name_col])

                # Redundancy (intra)
                red = self.redundancy_scorer.score_intra_program(cov["per_item_detail"])

                # Depth (Bloom)
                los = row.get("learning_outcomes", [])
                dep = self.depth_scorer.score_program(
                    desc, los if isinstance(los, list) else []
                )

                # Latency
                lat = self.latency_scorer.score_program(
                    desc,
                    row.get("program_age", 0),
                    row.get("frameworks_matched", 0),
                )

                # SOA density (from soa_results if available)
                soa_density = 0.0
                if self.soa_results and idx < len(self.soa_results):
                    soa = self.soa_results[idx]
                    matrix_len = len(soa["matrix"])
                    denom = max(1, soa["n_items_matched"] * soa["n_outcomes"] * soa["n_assessments"])
                    soa_density = min(matrix_len / denom, 1.0)

                # Composite
                n_out = len(row.get("learning_outcomes", []) or [])
                n_asm = len(row.get("assessment_methods", []) or [])
                comp = self.composite_scorer.compute(
                    coverage_score=cov["overall_coverage"],
                    redundancy_score=red["intra_redundancy"],
                    depth_score=dep["depth_score"],
                    latency_score=lat["latency_score"],
                    soa_density_score=soa_density,
                    n_outcomes=n_out,
                    n_assessments=n_asm,
                )

                # Assessment diversity: ratio of distinct assessment types
                total_possible_methods = len(self.config.assessment_methods)
                assessment_diversity = n_asm / max(total_possible_methods, 1)

                score_row = {
                    "program": row[name_col],
                    "university": row[uni_col],
                    "country": row.get("country_full", ""),
                    "overall_coverage": cov["overall_coverage"],
                    "items_covered": cov["items_covered"],
                    "items_total": cov["items_total"],
                    "intra_redundancy": red["intra_redundancy"],
                    "depth_score": dep["depth_score"],
                    "depth_index": dep["depth_index"],
                    "highest_bloom": dep["highest_level"],
                    "n_bloom_levels": dep["n_bloom_levels"],
                    "latency_score": lat["latency_score"],
                    "age_freshness": lat["age_freshness"],
                    "tech_modernity": lat["tech_modernity"],
                    "soa_density": round(soa_density, 4),
                    "assessment_diversity": round(assessment_diversity, 4),
                    "composite_score": comp["composite_score"],
                    "grade": comp["grade"],
                    # Analysis scores 
                    "overall_breadth": row.get("overall_breadth", None),
                    "overall_depth": row.get("overall_depth", None),
                    "coherence_score": row.get("coherence_score", None),
                    "immersion_index": row.get("immersion_index", None),
                    "embedding_mean_score": row.get("embedding_mean_score", None),
                    "n_outcomes": row.get("n_outcomes", 0),
                    "n_assessments": row.get("n_assessments", 0),
                }
                self.all_scores.append(score_row)

            # Inter-program redundancy
            inter_red = self.redundancy_scorer.score_inter_program(
                all_coverage_details, program_names
            )

            # Export scores
            scores_df = pd.DataFrame(self.all_scores).sort_values(
                "composite_score", ascending=False
            )
            self.exporter.export_scores(scores_df)

            # Export inter-program redundancy
            inter_path = str(self._out / "inter_program_redundancy.csv")
            if inter_red.get("commodity_items"):
                pd.DataFrame(inter_red["commodity_items"]).to_csv(inter_path, index=False)


        
        if cfg.run_validation and self.all_scores:
            logger.info("Stage: validation")
            stages_executed.append("validation")
            descriptions = self.courses_df[desc_col].tolist()
            
            nlp_sp = self.courses_df.get("nlp_role_scores", pd.Series([{}] * len(self.courses_df))).tolist()
            emb_sp = self.courses_df.get("embedding_role_scores", pd.Series([{}] * len(self.courses_df))).tolist()
            onto_sp = self.courses_df.get("ontology_boosted_scores", pd.Series([{}] * len(self.courses_df))).tolist()

            ablation_df = self.validator.ablation_test(
                descriptions, self.framework_items, nlp_sp, emb_sp, onto_sp
            )
            ablation_path = str(self._out / "ablation_results.csv")
            ablation_df.to_csv(ablation_path, index=False)

            
            def _flatten_evidence(ev) -> set[str]:
                """Flatten a {role/concept: [keyword, ...]} dict to a flat keyword set."""
                if not isinstance(ev, dict):
                    return set()
                out: set[str] = set()
                for kws in ev.values():
                    if isinstance(kws, (list, set)):
                        out.update(kws)
                return out

            match_sets: dict[str, list[set[str]]] = {}
            if "ecsf_evidence" in self.courses_df.columns:
                match_sets["ecsf"] = [
                    _flatten_evidence(r) for r in self.courses_df["ecsf_evidence"]
                ]
            if "nice_evidence" in self.courses_df.columns:
                match_sets["nice"] = [
                    _flatten_evidence(r) for r in self.courses_df["nice_evidence"]
                ]
            if "jrc_evidence" in self.courses_df.columns:
                match_sets["jrc"] = [
                    _flatten_evidence(r) for r in self.courses_df["jrc_evidence"]
                ]

            method_comp_df = pd.DataFrame()
            if len(match_sets) >= 2:
                method_comp_df = self.validator.method_comparison(match_sets)
                mc_path = str(self._out / "method_comparison.csv")
                method_comp_df.to_csv(mc_path, index=False)

            # Rank correlation: composite vs coverage
            scores_df = pd.DataFrame(self.all_scores)
            rank_corr = {}
            if "composite_score" in scores_df.columns and "overall_coverage" in scores_df.columns:
                try:
                    rank_corr = self.validator.rank_correlation(
                        scores_df["composite_score"].tolist(),
                        scores_df["overall_coverage"].tolist(),
                    )
                except Exception:
                    rank_corr = {"spearman_rho": 0.0, "p_value": 1.0}

            # Top-N stability
            rankings: dict[str, list[str]] = {}
            for col in ["composite_score", "overall_coverage", "depth_score"]:
                if col in scores_df.columns:
                    sorted_df = scores_df.sort_values(col, ascending=False)
                    rankings[col] = sorted_df["program"].tolist()
            stability = self.validator.top_n_stability(rankings) if len(rankings) >= 2 else {}

            # Full validation summary
            self.validation_summary = self.validator.run_full_validation(
                ablation_df, method_comp_df, rank_corr, stability
            )
            self.exporter.export_validation(self.validation_summary)

        stages_executed.append("feedback")
        logger.info("Stage: feedback/recommendations")
        all_ecsf_names = self.enisa_df["profile_title"].tolist()
        all_nice_list = self.nice_df.to_dict("records") if self.nice_df is not None else []
        name_col = "study_program_name" if "study_program_name" in self.courses_df.columns else "study program name"

        self.recommendations = []
        for i, (_, row) in enumerate(self.courses_df.iterrows()):
            score_dict = self.all_scores[i] if i < len(self.all_scores) else {}
            soa = self.soa_results[i] if i < len(self.soa_results) else {"matrix": []}
            recs = self.feedback.generate_recommendations(
                row[name_col],
                score_dict,
                soa,
                row.get("ecsf_roles", []) or [],
                row.get("nice_roles", []) or [],
                row.get("jrc_concepts", []) or [],
                all_ecsf_names,
                all_nice_list,
            )
            self.recommendations.append({"program": row[name_col], "recommendations": recs})

        self.exporter.export_recommendations(self.recommendations)

        if cfg.run_graph:
            logger.info("Stage: graph build/export")
            stages_executed.append("graph")
            gp = GraphPipeline(self.config)
            gp.run(
                self.courses_df, self.enisa_df,
                use_neo4j=(bool(cfg.neo4j_password)),
                export_graphml=True,
            )
        elif cfg.run_reporting:
            try:
                graphml_path = str(self._out / "cybersecurity_education_kg.graphml")
                export_networkx_graphml(
                    self.courses_df, self.enisa_df, graphml_path,
                    nice_df=self.nice_df,
                    taxonomy_df=self.taxonomy_df,
                )
                stages_executed.append("graph")
            except Exception as e:
                logger.warning("GraphML export failed: %s", e)

        if cfg.run_reporting:
            logger.info("Stage: reporting")
            stages_executed.append("reporting")
            elapsed = (datetime.now() - start).total_seconds()
            self._run_metadata = {
                "timestamp": start.isoformat(),
                "elapsed_seconds": round(elapsed, 1),
                "n_programs": len(self.courses_df),
                "n_framework_items": len(self.framework_items),
                "total_soa_entries": sum(len(s["matrix"]) for s in self.soa_results),
                "stages_executed": stages_executed,
            }
            if self.all_scores:
                scores_df = pd.DataFrame(self.all_scores).sort_values(
                    "composite_score", ascending=False
                )
                self.exporter.export_scores(scores_df)

            # Export pipeline summary, report, and artifact manifest
            self.exporter.export_pipeline_summary(self._run_metadata)
            self.exporter.export_pipeline_report(
                self._run_metadata, self.all_scores, self.validation_summary or None
            )
            self.exporter.export_artifact_manifest(
                self._run_metadata,
                {"courses": len(self.courses_df),
                 "enisa_roles": len(self.enisa_df),
                 "framework_items": len(self.framework_items),
                 "scores": len(self.all_scores),
                 "soa_entries": sum(len(s["matrix"]) for s in self.soa_results)},
            )

        logger.info("Pipeline finished in %.1fs", (datetime.now() - start).total_seconds())
        return self._run_metadata
    
    def get_scores_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.all_scores).sort_values("composite_score", ascending=False)

    def get_soa_flat(self) -> pd.DataFrame:
        return SkillAligner.flatten_all_soa(self.soa_results)
