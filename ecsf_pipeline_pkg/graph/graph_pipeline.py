"""
Graph Pipeline
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

from ..config import PipelineConfig

logger = logging.getLogger("ecsf_pipeline.graph")

# Build the course similarity graph in Neo4j
def build_course_graph(
    config: PipelineConfig,
    embeddings_path: str,
    metadata_path: str,
    dataset_path: str,
    clear: bool = False,
    top_k: int = 5,
    similarity_threshold: float = 0.5,
    batch_size: int = 100,
) -> dict:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "data_preprocessing" / "embeddings"))
    from create_neo4j_graph import (
        Neo4jGraphBuilder, load_embeddings, load_original_dataset,
        compute_similarity_matrix, get_top_similar_courses, parse_skills,
    )

    embeddings, metadata = load_embeddings(embeddings_path, metadata_path)
    df = load_original_dataset(dataset_path)
    builder = Neo4jGraphBuilder(config.neo4j_uri, config.neo4j_user, config.neo4j_password)

    try:
        if clear:
            builder.clear_database()
        builder.create_constraints()

        # Course nodes
        courses = []
        for idx, row in metadata.iterrows():
            row_index = int(row["row_index"])
            course_row = df.iloc[row_index]
            courses.append({
                "course_id": int(idx),
                "title": str(course_row.get("course_title", "")),
                "description": str(course_row.get("Description", course_row.get("description", ""))),
                "original_description": str(course_row.get("original_description", "")),
                "row_index": row_index,
                "embedding": embeddings[idx].tolist(),
            })
        for i in range(0, len(courses), batch_size):
            builder.batch_create_courses(courses[i:i + batch_size])

        # Skill nodes
        all_skills = set()
        course_skills = []
        for idx, row in df.iterrows():
            skills = parse_skills(row.get("extracted_skills", ""))
            for skill in skills:
                all_skills.add(skill)
                course_skills.append({"course_id": int(idx), "skill": skill})
        skills_list = list(all_skills)
        for i in range(0, len(skills_list), batch_size):
            builder.batch_create_skills(skills_list[i:i + batch_size])
        for i in range(0, len(course_skills), batch_size):
            builder.batch_create_course_skill_relationships(course_skills[i:i + batch_size])

        # Similarity
        sim_matrix = compute_similarity_matrix(embeddings)
        similar = get_top_similar_courses(sim_matrix, top_k=top_k, threshold=similarity_threshold)
        sim_rels = [{"course_id_1": int(c1), "course_id_2": int(c2), "similarity": float(s)}
                    for c1, c2, s in similar]
        for i in range(0, len(sim_rels), batch_size):
            builder.batch_create_similarity_relationships(sim_rels[i:i + batch_size])

        stats = {
            "course_nodes": len(courses),
            "skill_nodes": len(all_skills),
            "course_skill_edges": len(course_skills),
            "similarity_edges": len(sim_rels),
        }
        logger.info("Course graph built: %s", stats)
        return stats
    finally:
        builder.close()


# Build the ENISA profile graph in Neo4j
def build_profile_graph(
    config: PipelineConfig,
    enisa_csv_path: str,
    clear: bool = False,
    batch_size: int = 100,
    skill_sim_threshold: int = 2,
    knowledge_sim_threshold: int = 2,
) -> dict:


    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "data_preprocessing" / "embeddings"))
    from create_neo4j_enisa_graph import (
        EnisaGraphBuilder, load_enisa_dataset, parse_multiline_field,
    )

    df = load_enisa_dataset(enisa_csv_path)
    builder = EnisaGraphBuilder(config.neo4j_uri, config.neo4j_user, config.neo4j_password)

    try:
        if clear:
            builder.clear_database()
        builder.create_constraints()

        profiles = [
            {"profile_no": int(row["no"]), "title": str(row["profile_title"]),
             "mission": str(row.get("mission", "")), "main_tasks": str(row.get("main_tasks", ""))}
            for _, row in df.iterrows()
        ]
        for i in range(0, len(profiles), batch_size):
            builder.batch_create_profiles(profiles[i:i + batch_size])

        all_skills, profile_skills = set(), []
        all_knowledge, profile_knowledge = set(), []
        all_deliverables, profile_deliverables = set(), []

        for _, row in df.iterrows():
            title = str(row["profile_title"])
            for skill in parse_multiline_field(row.get("key_skills", "")):
                all_skills.add(skill)
                profile_skills.append({"profile_title": title, "skill": skill})
            for kn in parse_multiline_field(row.get("key_knowledge", "")):
                all_knowledge.add(kn)
                profile_knowledge.append({"profile_title": title, "knowledge": kn})
            for dl in parse_multiline_field(row.get("deliverables", "")):
                all_deliverables.add(dl)
                profile_deliverables.append({"profile_title": title, "deliverable": dl})

        for batch_items, create_fn in [
            (list(all_skills), builder.batch_create_skills),
            (list(all_knowledge), builder.batch_create_knowledge),
            (list(all_deliverables), builder.batch_create_deliverables),
        ]:
            for i in range(0, len(batch_items), batch_size):
                create_fn(batch_items[i:i + batch_size])

        for rels, create_fn in [
            (profile_skills, builder.batch_create_profile_skill_relationships),
            (profile_knowledge, builder.batch_create_profile_knowledge_relationships),
            (profile_deliverables, builder.batch_create_profile_deliverable_relationships),
        ]:
            for i in range(0, len(rels), batch_size):
                create_fn(rels[i:i + batch_size])

        builder.create_skill_similarity_relationships(skill_sim_threshold)
        builder.create_knowledge_similarity_relationships(knowledge_sim_threshold)

        stats = {
            "profile_nodes": len(profiles),
            "skill_nodes": len(all_skills),
            "knowledge_nodes": len(all_knowledge),
            "deliverable_nodes": len(all_deliverables),
        }
        logger.info("Profile graph built: %s", stats)
        return stats
    finally:
        builder.close()


# Integrate course and profile graphs — skill + knowledge matching
def integrate_graphs(
    config: PipelineConfig,
    skip_skill: bool = False,
    skip_knowledge: bool = False,
) -> dict:
    
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "data_preprocessing" / "embeddings"))
    from integrate_graphs import GraphIntegrator

    integrator = GraphIntegrator(config.neo4j_uri, config.neo4j_user, config.neo4j_password)
    try:
        graph_status = integrator.check_graphs_exist()
        if not graph_status["enisa_graph"] or not graph_status["course_graph"]:
            logger.warning("Missing graph(s): %s", graph_status)
            return {"error": "One or both graphs do not exist", "status": graph_status}

        stats: dict[str, Any] = {}
        if not skip_skill:
            stats["profile_course_by_skill"] = integrator.create_profile_course_relationships_by_skill()
            stats["skill_course"] = integrator.create_skill_course_relationships()
        if not skip_knowledge:
            stats["profile_course_by_knowledge"] = integrator.create_profile_course_relationships_by_knowledge()
            stats["knowledge_course"] = integrator.create_knowledge_course_relationships()
        stats.update(integrator.get_integration_statistics())
        logger.info("Graph integration complete: %s", stats)
        return stats
    finally:
        integrator.close()


# Build a NetworkX knowledge graph and export to GraphML
def export_networkx_graphml(
    courses_df: pd.DataFrame,
    enisa_df: pd.DataFrame,
    output_path: str = "cybersecurity_education_kg.graphml",
    nice_df: pd.DataFrame | None = None,
    taxonomy_df: pd.DataFrame | None = None,
) -> dict:
    import networkx as nx

    G = nx.DiGraph()

    # Center node and category hubs 
    G.add_node("Cybersecurity Education", node_type="center",
               label="Cybersecurity Education")
    for cat in ["ECSF Roles", "Universities", "Skills and Knowledge"]:
        G.add_node(cat, node_type="category", label=cat)
        G.add_edge("Cybersecurity Education", cat, edge_type="HAS_CATEGORY")

    # ECSF roles, skills, and knowledge items
    for _, row in enisa_df.iterrows():
        role = row["profile_title"]
        G.add_node(role, node_type="ecsf_role", label=role)
        G.add_edge("ECSF Roles", role, edge_type="HAS_MEMBER")

        for sk in row.get("key_skills_list", []):
            sk_id = f"skill:{sk[:60]}"
            if not G.has_node(sk_id):
                G.add_node(sk_id, node_type="skill", label=sk[:60])
            G.add_edge(role, sk_id, edge_type="REQUIRES_SKILL")

        for kn in row.get("key_knowledge_list", []):
            kn_id = f"knowledge:{kn[:60]}"
            if not G.has_node(kn_id):
                G.add_node(kn_id, node_type="knowledge", label=kn[:60])
            G.add_edge(role, kn_id, edge_type="REQUIRES_KNOWLEDGE")

    # NICE framework roles and categories (v2 layer)
    if nice_df is not None:
        nice_categories_added: set[str] = set()
        for _, row in nice_df.iterrows():
            role_name = row.get("role_name", "")
            category = row.get("category", "")
            if not G.has_node(role_name):
                G.add_node(role_name, node_type="nice_role", label=role_name,
                           nice_category=category)
            if category and category not in nice_categories_added:
                cat_id = f"nice_cat:{category}"
                G.add_node(cat_id, node_type="nice_category", label=category)
                nice_categories_added.add(category)
            if category:
                G.add_edge(f"nice_cat:{category}", role_name,
                           edge_type="HAS_WORK_ROLE")

    # JRC taxonomy concepts (v2 layer)
    if taxonomy_df is not None:
        for _, row in taxonomy_df.iterrows():
            label = row["label"]
            jrc_id = f"jrc:{label}"
            if not G.has_node(jrc_id):
                facet = row.get("facet", "General")
                is_top = bool(row.get("is_top_concept", False))
                ntype = "jrc_domain" if is_top else "jrc_concept"
                G.add_node(jrc_id, node_type=ntype, label=label, facet=facet)
            parent_label = row.get("parent_label", "")
            if parent_label:
                parent_id = f"jrc:{parent_label}"
                if G.has_node(parent_id):
                    G.add_edge(parent_id, jrc_id, edge_type="BROADER_THAN")

    # University / program nodes
    name_col = ("study_program_name" if "study_program_name" in courses_df.columns
                else "study program name")
    uni_col = ("university_name" if "university_name" in courses_df.columns
               else "university name")

    for _, row in courses_df.iterrows():
        prog = row[name_col]
        uni = row[uni_col]
        country = row.get("country_full", "")

        # University
        if not G.has_node(uni):
            G.add_node(uni, node_type="university", label=uni)
            G.add_edge("Universities", uni, edge_type="HAS_MEMBER")

        # Program node
        G.add_node(prog, node_type="program", label=prog,
                   university=uni, country=country)
        G.add_edge(uni, prog, edge_type="OFFERS")

        # ECSF role links
        for role in (row.get("ecsf_roles") or []):
            G.add_edge(prog, role, edge_type="PREPARES_FOR")

        # NICE role links
        for role in (row.get("nice_roles") or []):
            if not G.has_node(role):
                G.add_node(role, node_type="nice_role", label=role)
            G.add_edge(prog, role, edge_type="PREPARES_FOR_NICE")

        # JRC concept links
        for concept in (row.get("jrc_concepts") or []):
            jrc_id = f"jrc:{concept}"
            if not G.has_node(jrc_id):
                G.add_node(jrc_id, node_type="jrc_concept", label=concept)
            G.add_edge(prog, jrc_id, edge_type="COVERS_CONCEPT")

        # Country
        if country:
            if not G.has_node(country):
                G.add_node(country, node_type="country", label=country)
            G.add_edge(uni, country, edge_type="LOCATED_IN")

    nx.write_graphml(G, output_path)
    stats = {"nodes": G.number_of_nodes(), "edges": G.number_of_edges(),
             "path": output_path}
    logger.info("GraphML exported: %s", stats)
    return stats


# high-level wrapper that calls build/integrate/export in sequence
class GraphPipeline:

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.stats: dict[str, Any] = {}

    # Run the graph pipeline.
    def run(
        self,
        courses_df: pd.DataFrame,
        enisa_df: pd.DataFrame,
        embeddings_path: str | None = None,
        metadata_path: str | None = None,
        dataset_path: str | None = None,
        use_neo4j: bool = False,
        export_graphml: bool = True,
    ) -> dict:
        out_dir = Path(self.config.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        if use_neo4j and embeddings_path and metadata_path and dataset_path:
            self.stats["course_graph"] = build_course_graph(
                self.config, embeddings_path, metadata_path, dataset_path, clear=True)
            self.stats["profile_graph"] = build_profile_graph(
                self.config, self.config.enisa_csv, clear=False)
            self.stats["integration"] = integrate_graphs(self.config)

        if export_graphml:
            graphml_path = str(out_dir / "cybersecurity_education_kg.graphml")
            self.stats["graphml_export"] = export_networkx_graphml(
                courses_df, enisa_df, graphml_path)

        # Save provenance
        manifest_path = str(out_dir / "graph_exports_manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(self.stats, f, indent=2, default=str)
        logger.info("Graph manifest → %s", manifest_path)

        return self.stats
