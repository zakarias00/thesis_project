"""
Curriculum Feedback & Recommendation Generator
"""
from __future__ import annotations

import logging

from ..config import PipelineConfig

logger = logging.getLogger("ecsf_pipeline.feedback")


# generate prioritized improvement recommendations per program
class CurriculumFeedback:

    BLOOM_ORDER = ["Remember", "Understand", "Apply", "Analyze", "Evaluate", "Create"]

    def __init__(self, config: PipelineConfig):
        self.config = config

    def generate_recommendations(
        self,
        program_name: str,
        scores: dict,
        soa_result: dict,
        ecsf_roles: list,
        nice_roles: list,
        jrc_concepts: list,
        all_ecsf: list,
        all_nice: list,
    ) -> list[dict]:
        recs: list[dict] = []

        
        missing_ecsf = set(all_ecsf) - set(ecsf_roles or [])
        if missing_ecsf:
            recs.append({
                "category": "Framework Coverage",
                "priority": "High" if len(missing_ecsf) > 6 else "Medium",
                "finding": f"Program covers {len(ecsf_roles or [])}/{len(all_ecsf)} ECSF roles",
                "recommendation": f"Consider adding content for: {', '.join(list(missing_ecsf)[:5])}",
                "impact": "Broader workforce preparation",
            })

        missing_nice_cats = set()
        nice_cat_map: dict[str, list[str]] = {}
        for nr in all_nice:
            nice_cat_map.setdefault(nr.get("category", ""), []).append(nr.get("role_name", ""))
        for cat, roles in nice_cat_map.items():
            if not any(r in (nice_roles or []) for r in roles):
                missing_nice_cats.add(cat)
        if missing_nice_cats:
            recs.append({
                "category": "Framework Coverage",
                "priority": "Medium",
                "finding": f"No coverage for NICE categories: {', '.join(missing_nice_cats)}",
                "recommendation": "Introduce modules covering these NICE categories",
                "impact": "International framework alignment",
            })

        depth = scores.get("depth_score", scores.get("bloom_depth", 0))
        if depth < 0.5:
            recs.append({
                "category": "Learning Depth",
                "priority": "High",
                "finding": f"Bloom depth score = {depth:.2f} (below 0.50)",
                "recommendation": "Add higher-order activities (design, evaluate, create)",
                "impact": "Deeper competency acquisition",
            })

        assess_div = scores.get("assessment_diversity", 0)
        if assess_div < 0.3:
            recs.append({
                "category": "Assessment Methods",
                "priority": "Medium",
                "finding": f"Assessment diversity = {assess_div:.2f}",
                "recommendation": "Incorporate CTF exercises, internships, or case studies alongside exams",
                "impact": "More authentic assessment",
            })

        soa_density = scores.get("soa_density", 0)
        if soa_density < 0.1:
            recs.append({
                "category": "Curriculum Alignment",
                "priority": "High",
                "finding": f"SOA matrix density = {soa_density:.3f}",
                "recommendation": "Explicitly map learning outcomes to framework skills",
                "impact": "Traceability and accreditation readiness",
            })

        coverage = scores.get("overall_coverage", 0)
        if coverage < 0.3:
            recs.append({
                "category": "Content Coverage",
                "priority": "High",
                "finding": f"Overall coverage = {coverage:.2f}",
                "recommendation": "Expand curriculum to address more framework items",
                "impact": "Comprehensive skill development",
            })

        latency = scores.get("latency_score", 1.0)
        if latency < 0.4:
            recs.append({
                "category": "Curriculum Freshness",
                "priority": "Medium",
                "finding": f"Latency score = {latency:.2f}",
                "recommendation": "Update curriculum with modern tech references and recent frameworks",
                "impact": "Industry relevance",
            })

        return recs
