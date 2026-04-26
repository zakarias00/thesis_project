"""
Validation Suite
=================
Method comparison, ablation testing, rank correlation, and stability.
"""

from __future__ import annotations

import logging
from collections import defaultdict

import numpy as np
import pandas as pd

from ..config import PipelineConfig
from ..schemas import FrameworkItem

logger = logging.getLogger("ecsf_pipeline.validation")

STOP_WORDS = frozenset([
    "and", "the", "of", "to", "in", "for", "a", "an", "on", "with",
    "is", "are", "be", "or", "as", "by", "that", "this", "from",
    "their", "its", "can", "such", "may", "also", "etc", "will",
])


def _tokenize(text: str) -> set[str]:
    import re
    words = re.findall(r"[a-z]{3,}", str(text).lower())
    return {w for w in words if w not in STOP_WORDS}


class ValidationSuite:
    """Formal validation suite for the scoring pipeline.

    Computes:
      - Regex-only vs full multi-layer coverage improvement
      - Jaccard overlaps between framework-match sets from different methods
      - Spearman rank correlation between scoring variants
      - Ablation tests (regex / regex+NLP / regex+NLP+ontology / full)
      - Top-N stability analysis
    """

    def __init__(self, config: PipelineConfig):
        self.config = config

    # ────────────────────────────────────────────────────────────────────────
    # Ablation: run coverage under progressively richer method combinations
    # ────────────────────────────────────────────────────────────────────────

    def ablation_test(
        self,
        descriptions: list[str],
        framework_items: list[FrameworkItem],
        nlp_scores_per_program: list[dict] | None = None,
        embedding_scores_per_program: list[dict] | None = None,
        ontology_scores_per_program: list[dict] | None = None,
    ) -> pd.DataFrame:
        """Run ablation tests with four configurations.

        Configurations:
          - regex_only:          token-overlap only
          - regex_nlp:           + spaCy NLP role scores
          - regex_nlp_ontology:  + ontology-boosted role scores (graph neighbourhood)
          - full:                + sentence-transformer embedding scores

        Returns DataFrame with columns: program_idx, method, items_covered, coverage.
        """
        n = len(descriptions)
        nlp_scores_per_program = nlp_scores_per_program or [{}] * n
        embedding_scores_per_program = embedding_scores_per_program or [{}] * n
        ontology_scores_per_program = ontology_scores_per_program or [{}] * n

        # Pull thresholds from config
        regex_thresh = self.config.ablation_regex_threshold
        nlp_thresh = self.config.ablation_nlp_threshold
        onto_thresh = self.config.ablation_ontology_threshold
        emb_thresh = self.config.ablation_embedding_threshold

        rows: list[dict] = []
        for idx, desc in enumerate(descriptions):
            desc_tokens = _tokenize(desc) if not pd.isna(desc) else set()
            nlp_scores = nlp_scores_per_program[idx]
            emb_scores = embedding_scores_per_program[idx]
            onto_scores = ontology_scores_per_program[idx]

            for method_label, use_nlp, use_onto, use_emb in [
                ("regex_only",         False, False, False),
                ("regex_nlp",          True,  False, False),
                ("regex_nlp_ontology", True,  True,  False),
                ("full",               True,  True,  True),
            ]:
                covered = 0
                for item in framework_items:
                    item_tokens = _tokenize(item.label)
                    # Regex layer
                    regex_hit = (
                        len(desc_tokens & item_tokens) / max(len(item_tokens), 1)
                        >= regex_thresh
                        if item_tokens else False
                    )
                    # NLP layer
                    nlp_hit = (
                        use_nlp
                        and nlp_scores.get(item.parent, 0.0) >= nlp_thresh
                    )
                    # Ontology-boosted layer (uses graph-neighbourhood boost)
                    onto_hit = (
                        use_onto
                        and onto_scores.get(item.parent, 0.0) >= onto_thresh
                    )
                    # Embedding layer
                    emb_hit = (
                        use_emb
                        and emb_scores.get(item.parent, 0.0) >= emb_thresh
                    )
                    if regex_hit or nlp_hit or onto_hit or emb_hit:
                        covered += 1

                rows.append({
                    "program_idx": idx,
                    "method": method_label,
                    "items_covered": covered,
                    "coverage": round(covered / max(len(framework_items), 1), 4),
                })

        return pd.DataFrame(rows)

    # ────────────────────────────────────────────────────────────────────────
    # Method comparison: Jaccard between match sets
    # ────────────────────────────────────────────────────────────────────────

    def method_comparison(
        self,
        match_sets: dict[str, list[set[str]]],
    ) -> pd.DataFrame:
        """Compare framework-match sets across methods via Jaccard.

        Parameters
        ----------
        match_sets : dict[str, list[set[str]]]
            Keys are method names (e.g. "ecsf", "nice", "jrc").
            Values are lists of sets (one set per program).
        """
        methods = list(match_sets.keys())
        n_programs = len(next(iter(match_sets.values())))
        rows: list[dict] = []

        for i in range(len(methods)):
            for j in range(i + 1, len(methods)):
                jaccards = []
                for p in range(n_programs):
                    s1 = match_sets[methods[i]][p]
                    s2 = match_sets[methods[j]][p]
                    union = s1 | s2
                    jac = len(s1 & s2) / len(union) if union else 0.0
                    jaccards.append(jac)
                rows.append({
                    "method_a": methods[i],
                    "method_b": methods[j],
                    "mean_jaccard": round(float(np.mean(jaccards)), 4),
                    "median_jaccard": round(float(np.median(jaccards)), 4),
                    "std_jaccard": round(float(np.std(jaccards)), 4),
                })

        return pd.DataFrame(rows)

    # ────────────────────────────────────────────────────────────────────────
    # Rank correlation between scoring variants
    # ────────────────────────────────────────────────────────────────────────

    def rank_correlation(
        self,
        scores_a: list[float],
        scores_b: list[float],
    ) -> dict:
        """Spearman rank correlation between two scoring vectors."""
        from scipy.stats import spearmanr
        if len(scores_a) < 3 or len(scores_b) < 3:
            return {"spearman_rho": 0.0, "p_value": 1.0}
        rho, p = spearmanr(scores_a, scores_b)
        return {
            "spearman_rho": round(float(rho), 4),
            "p_value": round(float(p), 6),
        }

    # ────────────────────────────────────────────────────────────────────────
    # Top-N stability analysis
    # ────────────────────────────────────────────────────────────────────────

    def top_n_stability(
        self,
        rankings: dict[str, list[str]],
        n: int = 10,
    ) -> dict:
        """Overlap stability among top-N programs across different methods.

        Parameters
        ----------
        rankings : dict[str, list[str]]
            Keys are method names, values are ordered lists of program names.
        n : int
            How many top programs to compare.

        Returns
        -------
        dict with pairwise overlap ratios and average stability.
        """
        methods = list(rankings.keys())
        pairwise: list[dict] = []

        for i in range(len(methods)):
            for j in range(i + 1, len(methods)):
                top_i = set(rankings[methods[i]][:n])
                top_j = set(rankings[methods[j]][:n])
                overlap = len(top_i & top_j) / n if n > 0 else 0.0
                pairwise.append({
                    "method_a": methods[i],
                    "method_b": methods[j],
                    "overlap_ratio": round(overlap, 4),
                })

        avg_stability = (
            np.mean([p["overlap_ratio"] for p in pairwise]) if pairwise else 0.0
        )
        return {
            "n": n,
            "pairwise": pairwise,
            "average_stability": round(float(avg_stability), 4),
        }

    # ────────────────────────────────────────────────────────────────────────
    # Assemble full validation report
    # ────────────────────────────────────────────────────────────────────────

    def run_full_validation(
        self,
        ablation_df: pd.DataFrame,
        method_comparison_df: pd.DataFrame,
        rank_corr: dict,
        stability: dict,
    ) -> dict:
        """Consolidate all validation results into a single summary."""
        # Ablation summary: mean coverage per method
        ablation_summary = (
            ablation_df.groupby("method")["coverage"]
            .agg(["mean", "std"])
            .round(4)
            .to_dict("index")
        )
        # Coverage improvement from regex_only → full
        regex_mean = ablation_summary.get("regex_only", {}).get("mean", 0)
        full_mean = ablation_summary.get("full", {}).get("mean", 0)
        improvement = round(full_mean - regex_mean, 4)

        return {
            "ablation_summary": ablation_summary,
            "coverage_improvement_regex_to_full": improvement,
            "method_comparison": method_comparison_df.to_dict("records"),
            "rank_correlation": rank_corr,
            "top_n_stability": stability,
        }
