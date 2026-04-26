from __future__ import annotations

import re
import hashlib
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

# Course schema
COURSE_COLUMNS = [
    "course_id",
    "course_title",
    "description",
    "original_description",
    "combined_description",
    "extracted_skills",
    "university_name",
    "study_program_name",
    "country_full",
    "year_established",
    "program_age",
]

# Framework match output columns
FRAMEWORK_MATCH_COLUMNS = [
    "ecsf_roles",
    "ecsf_role_scores",
    "nice_roles",
    "nice_role_scores",
    "jrc_concepts",
    "jrc_knowledge_domains",
    "jrc_technologies",
    "frameworks_matched",
    "total_framework_roles",
]

# JRC taxonomy schema
JRC_COLUMNS = [
    "uri",
    "label",
    "definition",
    "facet",
    "is_top_concept",
    "parent_label",
    "broader_uris",
]

_COURSE_RENAME_MAP = {
    # Common raw CSV columns → canonical
    "course_title": "course_title",
    "course title": "course_title",
    "title": "course_title",
    "study program name": "study_program_name",
    "study_program_name": "study_program_name",
    "university name": "university_name",
    "university_name": "university_name",
    "study program description": "description",
    "description": "description",
    "Description": "description",
    "original_description": "original_description",
    "original description": "original_description",
    "combined_description": "combined_description",
    "combined description": "combined_description",
    "extracted_skills": "extracted_skills",
    "extracted skills": "extracted_skills",
    "country": "country_code",
    "country_full": "country_full",
    "year established": "year_established",
    "year_established": "year_established",
    "program_age": "program_age",
}

COUNTRY_MAP = {
    "prt": "Portugal", "grc": "Greece", "svn": "Slovenia", "ltu": "Lithuania",
    "swe": "Sweden", "cze": "Czech Republic", "esp": "Spain", "ita": "Italy",
    "aut": "Austria", "bel": "Belgium", "irl": "Ireland", "cyp": "Cyprus",
    "nld": "Netherlands", "che": "Switzerland", "fra": "France", "nor": "Norway",
    "lva": "Latvia", "fin": "Finland", "pol": "Poland", "isl": "Iceland",
    "otr": "Multi-country", "deu": "Germany",
}

@dataclass
class FrameworkItem:
    """Unified representation for any framework skill / knowledge / role item."""
    framework: str          # "ECSF" | "NICE" | "JRC"
    item_type: str          # "skill" | "knowledge" | "role" | "domain" | "technology"
    parent: str             # role or domain name this item belongs to
    label: str              # human-readable text
    source_id: Optional[str] = None

    @property
    def uid(self) -> str:
        """Deterministic unique id."""
        raw = f"{self.framework}|{self.item_type}|{self.parent}|{self.label}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]


def standardize_course_df(
    df: pd.DataFrame,
    reference_year: int = 2026,
) -> pd.DataFrame:
    
    df = df.copy()

    # Rename columns
    lower_map = {c: c.strip() for c in df.columns}
    df.rename(columns=lower_map, inplace=True)

    rename = {}
    for col in df.columns:
        canonical = _COURSE_RENAME_MAP.get(col)
        if canonical and canonical != col:
            rename[col] = canonical
    df.rename(columns=rename, inplace=True)

    # Derive country_full
    if "country_code" in df.columns and "country_full" not in df.columns:
        df["country_full"] = df["country_code"].map(COUNTRY_MAP).fillna(df["country_code"])
    elif "country" in df.columns and "country_full" not in df.columns:
        df["country_full"] = df["country"].map(COUNTRY_MAP).fillna(df["country"])

    # Program age
    if "year_established" in df.columns:
        df["program_age"] = reference_year - pd.to_numeric(
            df["year_established"], errors="coerce"
        )

    # ── Deterministic course_id ─────────────────────────────────────────────
    if "course_id" not in df.columns:
        def _make_id(row):
            src = f"{row.get('university_name', '')}|{row.get('study_program_name', '')}|{row.get('course_title', '')}"
            return hashlib.md5(src.encode()).hexdigest()[:10]
        df["course_id"] = df.apply(_make_id, axis=1)

    # Strip text columns
    text_cols = [
        "university_name", "study_program_name", "description",
        "original_description", "combined_description", "course_title",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def schema_tolerant_get(row: pd.Series, *keys, default=""):    
    for k in keys:
        val = row.get(k)
        if val is not None and not (isinstance(val, float) and pd.isna(val)):
            return val
    return default


# Build a list of FrameworkItems from the ENISA ECSF skill-set table
def build_framework_items_ecsf(enisa_df: pd.DataFrame) -> list[FrameworkItem]:
    items: list[FrameworkItem] = []
    for _, row in enisa_df.iterrows():
        role = row["profile_title"]
        for sk in row.get("key_skills_list", []):
            items.append(FrameworkItem("ECSF", "skill", role, sk))
        for kn in row.get("key_knowledge_list", []):
            items.append(FrameworkItem("ECSF", "knowledge", role, kn))
    return items


# Build FrameworkItems from the NICE work-role table
def build_framework_items_nice(nice_df: pd.DataFrame) -> list[FrameworkItem]:
    items: list[FrameworkItem] = []
    for _, row in nice_df.iterrows():
        role_name = row.get("role_name", "")
        category = row.get("category", "")
        items.append(
            FrameworkItem(
                "NICE", "role", category, role_name,
                source_id=row.get("role_id"),
            )
        )
    return items


# Build FrameworkItems from the JRC taxonomy concept table
def build_framework_items_jrc(taxonomy_df: pd.DataFrame) -> list[FrameworkItem]:
    items: list[FrameworkItem] = []
    for _, row in taxonomy_df.iterrows():
        facet = row.get("facet", "General")
        is_top = row.get("is_top_concept") or row.get("is_top", False)
        itype = "domain" if is_top else "technology"
        items.append(
            FrameworkItem(
                "JRC", itype, facet, row["label"],
                source_id=row.get("uri"),
            )
        )
    return items
