"""
Scoring Module — Five-metric quality scoring suite
====================================================
CoverageScoreCalculator      – full-inventory item coverage
RedundancyScoreCalculator    – intra / inter-program redundancy
SkillDepthScoreCalculator    – Bloom-based cognitive depth
UpdateLatencyScoreCalculator – curriculum freshness
CompositeQualityScoreCalculator – weighted fusion + letter grade
"""

from __future__ import annotations

import re
import logging
from collections import Counter

import numpy as np
import pandas as pd

from ..config import PipelineConfig
from ..schemas import FrameworkItem

logger = logging.getLogger("ecsf_pipeline.scoring")

STOP_WORDS = frozenset([
    "and", "the", "of", "to", "in", "for", "a", "an", "on", "with",
    "is", "are", "be", "or", "as", "by", "that", "this", "from",
    "their", "its", "can", "such", "may", "also", "etc", "will",
])

BLOOM_ORDER = ["Remember", "Understand", "Apply", "Analyze", "Evaluate", "Create"]
BLOOM_LEVEL_NUM = {b: i + 1 for i, b in enumerate(BLOOM_ORDER)}


def _tokenize(text: str) -> set[str]:
    words = re.findall(r"[a-z]{3,}", str(text).lower())
    return {w for w in words if w not in STOP_WORDS}


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Coverage Score Calculator
# ═══════════════════════════════════════════════════════════════════════════════

class CoverageScoreCalculator:
    """Full-inventory coverage score using four evidence layers:
    regex/token overlap, NLP lemma overlap, ontology-boosted alignment,
    and embedding similarity.

    Computes per-item fused scores and an overall boolean-OR coverage ratio.
    """

    # Fused score weights (thesis Eq. fused)
    FUSED_WEIGHTS = {"regex": 0.15, "nlp": 0.25, "ontology": 0.20, "embedding": 0.40}

    def __init__(self, config: PipelineConfig):
        self.config = config

    def score_program(
        self,
        description: str,
        framework_items: list[FrameworkItem],
        nlp_role_scores: dict[str, float] | None = None,
        embedding_role_scores: dict[str, float] | None = None,
        ontology_role_scores: dict[str, float] | None = None,
    ) -> dict:
        """Score a single program against all framework items.

        Uses four evidence layers and computes both per-item fused scores
        and an overall boolean-OR coverage ratio.

        Parameters
        ----------
        description : str
            Combined program description text.
        framework_items : list[FrameworkItem]
            All ECSF + NICE + JRC items to evaluate against.
        nlp_role_scores : dict, optional
            Per-role NLP lemma-overlap scores (from NLPCompetencyExtractor).
        embedding_role_scores : dict, optional
            Per-role embedding cosine-similarity scores (from EmbeddingAnalyzer).
        ontology_role_scores : dict, optional
            Per-role ontology-boosted scores (from OntologyAligner).

        Returns
        -------
        dict with keys: overall_coverage, items_covered, items_total,
             per_item_detail, method_contributions
        """
        if pd.isna(description) or not framework_items:
            return {
                "overall_coverage": 0.0,
                "items_covered": 0,
                "items_total": len(framework_items),
                "per_item_detail": [],
                "method_contributions": {"regex": 0, "nlp": 0, "ontology": 0, "embedding": 0},
            }

        nlp_role_scores = nlp_role_scores or {}
        embedding_role_scores = embedding_role_scores or {}
        ontology_role_scores = ontology_role_scores or {}

        desc_tokens = _tokenize(description)
        per_item: list[dict] = []
        covered_count = 0
        method_hits = {"regex": 0, "nlp": 0, "ontology": 0, "embedding": 0}

        for item in framework_items:
            item_tokens = _tokenize(item.label)

            # Layer 1: regex/token overlap
            overlap = desc_tokens & item_tokens
            regex_overlap = len(overlap) / max(len(item_tokens), 1) if item_tokens else 0.0

            # Layer 2: NLP lemma overlap (role-level proxy)
            nlp_overlap = nlp_role_scores.get(item.parent, 0.0)

            # Layer 3: ontology-boosted alignment (role-level proxy)
            ontology_score = ontology_role_scores.get(item.parent, 0.0)

            # Layer 4: embedding similarity (role-level proxy)
            embedding_sim = embedding_role_scores.get(item.parent, 0.0)

            # Fused composite score: weighted combination of all four layers
            fused_score = (
                self.FUSED_WEIGHTS["regex"] * regex_overlap
                + self.FUSED_WEIGHTS["nlp"] * nlp_overlap
                + self.FUSED_WEIGHTS["ontology"] * ontology_score
                + self.FUSED_WEIGHTS["embedding"] * embedding_sim
            )

            # Covered if any layer exceeds its threshold
            covered_by: list[str] = []
            if regex_overlap >= self.config.ablation_regex_threshold:
                covered_by.append("regex")
                method_hits["regex"] += 1
            if nlp_overlap >= self.config.ablation_nlp_threshold:
                covered_by.append("nlp")
                method_hits["nlp"] += 1
            if ontology_score >= self.config.ablation_ontology_threshold:
                covered_by.append("ontology")
                method_hits["ontology"] += 1
            if embedding_sim >= self.config.ablation_embedding_threshold:
                covered_by.append("embedding")
                method_hits["embedding"] += 1

            is_covered = len(covered_by) > 0
            if is_covered:
                covered_count += 1

            per_item.append({
                "framework": item.framework,
                "item_type": item.item_type,
                "parent": item.parent,
                "label": item.label[:80],
                "regex_overlap": round(regex_overlap, 4),
                "nlp_overlap": round(nlp_overlap, 4),
                "ontology_score": round(ontology_score, 4),
                "embedding_similarity": round(embedding_sim, 4),
                "fused_score": round(fused_score, 4),
                "covered": is_covered,
                "covered_by": covered_by,
            })

        total = len(framework_items)
        coverage = covered_count / max(total, 1)

        return {
            "overall_coverage": round(coverage, 4),
            "items_covered": covered_count,
            "items_total": total,
            "per_item_detail": per_item,
            "method_contributions": method_hits,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Redundancy Score Calculator
# ═══════════════════════════════════════════════════════════════════════════════

class RedundancyScoreCalculator:
    """Intra-program and inter-program redundancy analysis."""

    def __init__(self, config: PipelineConfig):
        self.config = config

    def score_intra_program(
        self,
        coverage_detail: list[dict],
    ) -> dict:
        """Compute intra-program redundancy: how many framework items are
        covered by multiple roles (Jaccard overlap across roles' covered sets).

        Parameters
        ----------
        coverage_detail : list[dict]
            ``per_item_detail`` output from CoverageScoreCalculator.
        """
        role_sets: dict[str, set[str]] = {}
        for item in coverage_detail:
            if item["covered"]:
                role_sets.setdefault(item["parent"], set()).add(item["label"])

        pairwise: list[dict] = []
        roles = sorted(role_sets.keys())
        for i in range(len(roles)):
            for j in range(i + 1, len(roles)):
                s1, s2 = role_sets[roles[i]], role_sets[roles[j]]
                union = s1 | s2
                jaccard = len(s1 & s2) / len(union) if union else 0.0
                pairwise.append({
                    "role_a": roles[i],
                    "role_b": roles[j],
                    "shared_items": len(s1 & s2),
                    "jaccard": round(jaccard, 4),
                })

        avg_jaccard = (
            np.mean([p["jaccard"] for p in pairwise]) if pairwise else 0.0
        )
        return {
            "intra_redundancy": round(float(avg_jaccard), 4),
            "pairwise_overlaps": pairwise,
            "n_roles_covered": len(roles),
        }

    def score_inter_program(
        self,
        all_coverage_details: list[list[dict]],
        program_names: list[str],
    ) -> dict:
        """Inter-program redundancy: identify commodity vs differentiator items.

        An item is a *commodity* if covered by ≥ 75 % of programs.
        An item is a *differentiator* if covered by ≤ 25 % of programs.
        """
        item_counts: Counter = Counter()
        total_programs = len(all_coverage_details)

        for detail in all_coverage_details:
            for item in detail:
                if item["covered"]:
                    item_counts[item["label"]] += 1

        commodities, differentiators = [], []
        for label, count in item_counts.items():
            ratio = count / max(total_programs, 1)
            if ratio >= 0.75:
                commodities.append({"label": label, "program_count": count, "ratio": round(ratio, 3)})
            elif ratio <= 0.25:
                differentiators.append({"label": label, "program_count": count, "ratio": round(ratio, 3)})

        return {
            "inter_program_summary": {
                "total_programs": total_programs,
                "commodities": len(commodities),
                "differentiators": len(differentiators),
            },
            "commodity_items": sorted(commodities, key=lambda x: -x["ratio"]),
            "differentiator_items": sorted(differentiators, key=lambda x: x["ratio"]),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Skill Depth Score Calculator
# ═══════════════════════════════════════════════════════════════════════════════

class SkillDepthScoreCalculator:
    """Bloom-taxonomy-based cognitive depth scoring."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._verb_level: dict[str, str] = {}
        for level, verbs in config.bloom_verbs.items():
            for v in verbs:
                self._verb_level[v] = level

    def score_program(
        self,
        description: str,
        learning_outcomes: list[dict] | None = None,
    ) -> dict:
        """Compute Bloom-based depth score.

        Parameters
        ----------
        description : str
            Full program description.
        learning_outcomes : list[dict], optional
            Pre-extracted LOs with 'bloom_level' keys.
        """
        bloom_distribution: dict[str, int] = {l: 0 for l in BLOOM_ORDER}

        # From extracted LOs
        if learning_outcomes:
            for lo in learning_outcomes:
                lvl = lo.get("bloom_level", "Apply")
                if lvl in bloom_distribution:
                    bloom_distribution[lvl] += 1

        # From description text directly (augment)
        if not pd.isna(description):
            text = str(description).lower()
            for verb, level in self._verb_level.items():
                if re.search(r"\b" + re.escape(verb) + r"\w*\b", text):
                    bloom_distribution[level] += 1

        total_verbs = sum(bloom_distribution.values())
        if total_verbs == 0:
            return {
                "depth_score": 0.0,
                "depth_index": 0.0,
                "bloom_distribution": bloom_distribution,
                "highest_level": "None",
                "n_bloom_levels": 0,
            }

        # Weighted average: Remember=1 .. Create=6
        weighted_sum = sum(
            bloom_distribution[l] * BLOOM_LEVEL_NUM[l] for l in BLOOM_ORDER
        )
        depth_index = weighted_sum / (total_verbs * 6.0)  # normalize to [0, 1]

        # Highest achieved level
        highest = "Remember"
        for level in reversed(BLOOM_ORDER):
            if bloom_distribution[level] > 0:
                highest = level
                break

        n_levels = sum(1 for v in bloom_distribution.values() if v > 0)

        # Depth score: index weighted by diversity
        diversity_bonus = min(n_levels / 6.0, 1.0) * 0.15
        depth_score = min(depth_index + diversity_bonus, 1.0)

        return {
            "depth_score": round(depth_score, 4),
            "depth_index": round(depth_index, 4),
            "bloom_distribution": bloom_distribution,
            "highest_level": highest,
            "n_bloom_levels": n_levels,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Update Latency Score Calculator
# ═══════════════════════════════════════════════════════════════════════════════

class UpdateLatencyScoreCalculator:
    """Curriculum freshness score based on program age, modern technology
    term mentions, and recent framework references."""

    def __init__(self, config: PipelineConfig):
        self.config = config

    def score_program(
        self,
        description: str,
        program_age: float | int,
        frameworks_matched: int = 0,
    ) -> dict:
        """Compute latency/freshness score.

        Parameters
        ----------
        description : str
        program_age : numeric
            Years since program was established.
        frameworks_matched : int
            Number of frameworks (0–3) the program maps to.
        """
        # ── Age freshness (newer = higher) ──────────────────────────────────
        if pd.isna(program_age) or program_age <= 0:
            age_freshness = 0.5  # neutral
        else:
            # Linear decay: age_score(p) = max(0, 1 − program_age(p) / 20)
            age_freshness = max(0.0, 1.0 - float(program_age) / 20.0)

        # ── Modern tech term mentions ───────────────────────────────────────
        tech_terms_found: list[str] = []
        if not pd.isna(description):
            text = str(description).lower()
            for term in self.config.modern_tech_keywords:
                if term.lower() in text:
                    tech_terms_found.append(term)
        tech_modernity = min(len(tech_terms_found) / 10.0, 1.0)

        # ── Framework reference currency ────────────────────────────────────
        reference_currency = min(frameworks_matched / 3.0, 1.0)

        # ── Recent framework-specific references ────────────────────────────
        recent_refs_found: list[str] = []
        recent_patterns = [
            "nis2", "iso 27001:2022", "nist csf 2.0", "dora",
            "cyber resilience act", "ecsf 2022",
            "nice 2.0", "2024", "2025", "2026",
        ]
        if not pd.isna(description):
            text = str(description).lower()
            for ref in recent_patterns:
                if ref in text:
                    recent_refs_found.append(ref)
        ref_bonus = min(len(recent_refs_found) / 5.0, 0.15)

        # ── Composite ──────────────────────────────────────────────────────
        latency_score = (
            age_freshness * 0.30
            + tech_modernity * 0.35
            + reference_currency * 0.20
            + ref_bonus
        )
        latency_score = round(min(latency_score, 1.0), 4)

        return {
            "latency_score": latency_score,
            "age_freshness": round(age_freshness, 4),
            "reference_currency": round(reference_currency, 4),
            "tech_modernity": round(tech_modernity, 4),
            "references_found": recent_refs_found,
            "tech_terms_found": tech_terms_found,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Composite Quality Score Calculator
# ═══════════════════════════════════════════════════════════════════════════════

class CompositeQualityScoreCalculator:
    """Weighted fusion of all five scores → letter grade A–F."""

    def __init__(self, config: PipelineConfig):
        self.config = config

    def compute(
        self,
        coverage_score: float,
        redundancy_score: float,
        depth_score: float,
        latency_score: float,
        soa_density_score: float = 0.0,
        n_outcomes: int = 0,
        n_assessments: int = 0,
    ) -> dict:
        """Compute the composite quality score and letter grade.

        Parameters
        ----------
        coverage_score : float [0, 1]
        redundancy_score : float [0, 1]  — low is good (inverted internally)
        depth_score : float [0, 1]
        latency_score : float [0, 1]
        soa_density_score : float [0, 1]  — SOA matrix density metric
        n_outcomes : int — number of extracted LOs (used for adaptive weighting)
        n_assessments : int — number of extracted assessment methods

        Returns
        -------
        dict with composite_score, grade, component_scores
        """
        w = dict(self.config.composite_weights)  # copy so we don't mutate config
        # Redundancy: lower is better, so invert it
        inverted_redundancy = 1.0 - min(redundancy_score, 1.0)

        # ── Adaptive weighting ─────────────────────────────────────────────
        # When the SOA density is near-zero because the description lacked
        # extractable LOs/assessments (a data-availability issue, not a
        # quality signal), redistribute the SOA weight to other metrics
        # rather than silently penalising the program.
        soa_w = w.get("composite_soa", 0.25)
        if n_outcomes < 2 and n_assessments < 2 and soa_density_score < 0.01:
            # Redistribute SOA weight proportionally to other components
            other_keys = [k for k in w if k != "composite_soa"]
            other_total = sum(w[k] for k in other_keys)
            if other_total > 0:
                for k in other_keys:
                    w[k] += soa_w * (w[k] / other_total)
            soa_w = 0.0

        composite = (
            coverage_score * w.get("coverage", 0.25)
            + inverted_redundancy * w.get("redundancy", 0.15)
            + depth_score * w.get("depth", 0.20)
            + latency_score * w.get("latency", 0.15)
            + soa_density_score * soa_w
        )
        composite = round(min(composite, 1.0), 4)

        grade = "F"
        for letter, threshold in sorted(
            self.config.grade_thresholds.items(), key=lambda x: -x[1]
        ):
            if composite >= threshold:
                grade = letter
                break

        return {
            "composite_score": composite,
            "grade": grade,
            "component_scores": {
                "coverage": round(coverage_score, 4),
                "redundancy_raw": round(redundancy_score, 4),
                "redundancy_inverted": round(inverted_redundancy, 4),
                "depth": round(depth_score, 4),
                "latency": round(latency_score, 4),
                "soa_density": round(soa_density_score, 4),
            },
        }
