#!/usr/bin/env python3
"""
Convert the cybersecurity education knowledge graph (GraphML) to D3.js JSON
and render high-quality PNG / SVG exports for the full graph and several
subgraphs
-----------
    # From repo root
    python -m ecsf_pipeline_pkg.graph.convert_graphml_to_d3 \\
        --input  pipeline_output/cybersecurity_education_kg_v2.graphml \\
        --outdir pipeline_output/graph_figures

    # Quick defaults (reads from pipeline_output/, writes to pipeline_output/graph_figures/)
    python -m ecsf_pipeline_pkg.graph.convert_graphml_to_d3
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")                     # headless backend – no display needed
import matplotlib.pyplot as plt            # noqa: E402
import matplotlib.patches as mpatches      # noqa: E402
import networkx as nx                      # noqa: E402

# Visual configuration
NODE_CONFIG: dict[str, dict[str, Any]] = {
    "center":        {"color": "#1a1a2e", "radius": 28, "group": 0},
    "category":      {"color": "#16213e", "radius": 20, "group": 1},
    "ecsf_role":     {"color": "#e94560", "radius": 14, "group": 2},
    "nice_role":     {"color": "#f59e0b", "radius": 10, "group": 3},
    "nice_category": {"color": "#d97706", "radius": 16, "group": 4},
    "university":    {"color": "#0f3460", "radius": 11, "group": 5},
    "program":       {"color": "#2196F3", "radius": 10, "group": 5},
    "country":       {"color": "#533483", "radius": 13, "group": 6},
    "skill":         {"color": "#00b4d8", "radius": 7,  "group": 7},
    "knowledge":     {"color": "#48cae4", "radius": 7,  "group": 8},
    "jrc_domain":    {"color": "#10b981", "radius": 12, "group": 9},
    "jrc_concept":   {"color": "#34d399", "radius": 5,  "group": 10},
}
_DEFAULT_NODE = {"color": "#adb5bd", "radius": 8, "group": 99}

EDGE_CONFIG: dict[str, dict[str, Any]] = {
    "HAS_CATEGORY":       {"color": "#f0f6fc", "width": 3,   "dash": ""},
    "HAS_MEMBER":         {"color": "#e94560", "width": 2,   "dash": ""},
    "INCLUDES_ROLE":      {"color": "#e94560", "width": 2,   "dash": ""},
    "REQUIRES_SKILL":     {"color": "#00b4d8", "width": 1,   "dash": ""},
    "REQUIRES_KNOWLEDGE": {"color": "#48cae4", "width": 1,   "dash": ""},
    "PREPARES_FOR":       {"color": "#e94560", "width": 1.5, "dash": "4,3"},
    "PREPARES_FOR_NICE":  {"color": "#f59e0b", "width": 1,   "dash": "4,3"},
    "HAS_WORK_ROLE":      {"color": "#d97706", "width": 1,   "dash": ""},
    "OFFERS":             {"color": "#0f3460", "width": 1.5, "dash": ""},
    "LOCATED_IN":         {"color": "#533483", "width": 1,   "dash": "6,3"},
    "BROADER_THAN":       {"color": "#10b981", "width": 0.5, "dash": ""},
    "COVERS_CONCEPT":     {"color": "#34d399", "width": 1,   "dash": "3,3"},
}
_DEFAULT_EDGE = {"color": "#999999", "width": 1, "dash": ""}

# Matplotlib palette (opaque, dark-background friendly)
_MPL_PALETTE: dict[str, str] = {
    "center":        "#f0f6fc",
    "category":      "#8b949e",
    "ecsf_role":     "#e94560",
    "nice_role":     "#f59e0b",
    "nice_category": "#d97706",
    "university":    "#3b82f6",
    "program":       "#60a5fa",
    "country":       "#a78bfa",
    "skill":         "#22d3ee",
    "knowledge":     "#67e8f9",
    "jrc_domain":    "#34d399",
    "jrc_concept":   "#6ee7b7",
}

_MPL_SIZE: dict[str, int] = {
    "center":        4200,
    "category":      2800,
    "ecsf_role":     1400,
    "nice_role":     900,
    "nice_category": 1200,
    "university":    1000,
    "program":       900,
    "country":       1100,
    "skill":         700,
    "knowledge":     700,
    "jrc_domain":    1000,
    "jrc_concept":   500,
}


def _clean(text: str) -> str:
    """Collapse newlines embedded in GraphML ids."""
    return re.sub(r"\s*\n\s*", " ", text).strip()


def _short(label: str, maxlen: int = 40) -> str:
    return label if len(label) <= maxlen else label[: maxlen - 2] + ".."


def graphml_to_d3_json(input_path: str | Path, output_path: str | Path) -> dict:
    """Read a GraphML file and write a D3-force-compatible JSON file."""
    G = nx.read_graphml(str(input_path))
    node_index: dict[str, int] = {}
    nodes: list[dict] = []

    for i, (nid, attrs) in enumerate(G.nodes(data=True)):
        ntype = attrs.get("node_type", "unknown")
        cfg = NODE_CONFIG.get(ntype, _DEFAULT_NODE)
        label = attrs.get("label") or _clean(nid)
        nodes.append({
            "id": _clean(nid),
            "label": _short(label, 60),
            "node_type": ntype,
            "group": cfg["group"],
            "color": cfg["color"],
            "radius": cfg["radius"],
            "description": attrs.get("description", ""),
            "mission": attrs.get("mission", ""),
            "country": attrs.get("country", ""),
            "framework": attrs.get("framework", ""),
            "category": attrs.get("category", ""),
        })
        node_index[nid] = i

    links: list[dict] = []
    for u, v, attrs in G.edges(data=True):
        etype = attrs.get("edge_type", "unknown")
        cfg = EDGE_CONFIG.get(etype, _DEFAULT_EDGE)
        links.append({
            "source": node_index[u],
            "target": node_index[v],
            "edge_type": etype,
            "color": cfg["color"],
            "width": cfg["width"],
            "dash": cfg["dash"],
            "weight": attrs.get("weight", 1),
        })

    data = {"nodes": nodes, "links": links}
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"  ✓ D3 JSON  {len(nodes)} nodes, {len(links)} links → {out}")
    return data

def _build_nx(data: dict) -> nx.DiGraph:
    """Build a directed NetworkX graph from the D3 JSON structure."""
    id_by_idx = {i: n["id"] for i, n in enumerate(data["nodes"])}
    G = nx.DiGraph()
    for n in data["nodes"]:
        G.add_node(n["id"], **n)
    for e in data["links"]:
        src = id_by_idx[e["source"]] if isinstance(e["source"], int) else e["source"]
        tgt = id_by_idx[e["target"]] if isinstance(e["target"], int) else e["target"]
        if src != tgt:
            G.add_edge(src, tgt, **e)
    return G


def _neighbours(G: nx.DiGraph, seeds: set[str], depth: int = 1) -> set[str]:
    """Return *seeds* plus their neighbours up to *depth* hops (undirected)."""
    keep = set(seeds)
    frontier = set(seeds)
    for _ in range(depth):
        nxt: set[str] = set()
        for n in frontier:
            nxt |= set(G.predecessors(n)) | set(G.successors(n))
        frontier = nxt - keep
        keep |= frontier
    return keep


def _top_by_degree(G: nx.DiGraph, node_type: str, limit: int) -> list[str]:
    """Return the *limit* highest-degree nodes of a given type."""
    cands = [(n, G.degree(n)) for n, a in G.nodes(data=True) if a.get("node_type") == node_type]
    cands.sort(key=lambda x: x[1], reverse=True)
    return [n for n, _ in cands[:limit]]


def _nodes_of_type(G: nx.DiGraph, *types: str) -> set[str]:
    return {n for n, a in G.nodes(data=True) if a.get("node_type") in types}


def subgraph_overview(G: nx.DiGraph) -> nx.DiGraph:
    """Structural skeleton: center → categories → ECSF roles, NICE categories, countries."""
    keep = _nodes_of_type(G, "center", "category", "ecsf_role", "nice_category", "country")
    return G.subgraph(keep).copy()


def subgraph_ecsf_roles(G: nx.DiGraph) -> nx.DiGraph:
    """ECSF roles with their top skills & knowledge items."""
    seeds = _nodes_of_type(G, "center", "ecsf_role")
    # Add category node
    seeds |= {n for n in _nodes_of_type(G, "category") if "ECSF" in n or "Skill" in n}
    # Add top skills & knowledge connected to ECSF roles
    for role in list(_nodes_of_type(G, "ecsf_role")):
        neighbours = set(G.successors(role)) | set(G.predecessors(role))
        skills = [n for n in neighbours if G.nodes[n].get("node_type") == "skill"]
        knowledge = [n for n in neighbours if G.nodes[n].get("node_type") == "knowledge"]
        # Keep top 3 skills and top 2 knowledge per role for readability
        skills.sort(key=lambda n: G.degree(n), reverse=True)
        knowledge.sort(key=lambda n: G.degree(n), reverse=True)
        seeds.update(skills[:3])
        seeds.update(knowledge[:2])
    return G.subgraph(seeds).copy()


def subgraph_nice_framework(G: nx.DiGraph) -> nx.DiGraph:
    """NICE categories → work roles."""
    seeds = _nodes_of_type(G, "center", "nice_category", "nice_role")
    seeds |= {n for n in _nodes_of_type(G, "category") if "ECSF" not in n and "Univ" not in n}
    return G.subgraph(seeds).copy()


def subgraph_universities(G: nx.DiGraph) -> nx.DiGraph:
    """Universities → programs → countries."""
    seeds = _nodes_of_type(G, "center", "university", "program", "country")
    seeds |= {n for n in _nodes_of_type(G, "category") if "Univ" in n}
    return G.subgraph(seeds).copy()


def subgraph_jrc_taxonomy(G: nx.DiGraph) -> nx.DiGraph:
    """JRC domains plus top concepts per domain (capped for readability)."""
    seeds = _nodes_of_type(G, "jrc_domain")
    # For each domain keep top-5 concepts by degree
    for dom in list(seeds):
        nbrs = set(G.successors(dom)) | set(G.predecessors(dom))
        concepts = [n for n in nbrs if G.nodes[n].get("node_type") == "jrc_concept"]
        concepts.sort(key=lambda n: G.degree(n), reverse=True)
        seeds.update(concepts[:5])
    return G.subgraph(seeds).copy()


def subgraph_role_program(G: nx.DiGraph) -> nx.DiGraph:
    """Alignment view: ECSF roles ↔ programs that prepare for them."""
    seeds = _nodes_of_type(G, "center", "ecsf_role", "program")
    # keep only programs that actually connect to an ECSF role
    ecsf = _nodes_of_type(G, "ecsf_role")
    connected_programs: set[str] = set()
    for role in ecsf:
        nbrs = set(G.predecessors(role)) | set(G.successors(role))
        connected_programs |= {n for n in nbrs if G.nodes[n].get("node_type") == "program"}
    # Also keep universities that OFFER those programs
    unis: set[str] = set()
    for prog in connected_programs:
        nbrs = set(G.predecessors(prog)) | set(G.successors(prog))
        unis |= {n for n in nbrs if G.nodes[n].get("node_type") == "university"}
    seeds = ecsf | connected_programs | unis | _nodes_of_type(G, "center")
    seeds |= {n for n in _nodes_of_type(G, "category") if "ECSF" in n or "Univ" in n}
    return G.subgraph(seeds).copy()


SUBGRAPH_REGISTRY: dict[str, tuple[Any, str]] = {
    "overview":       (subgraph_overview,       "Structural Overview – ECSF Roles, NICE Categories & Countries"),
    "ecsf_roles":     (subgraph_ecsf_roles,     "ECSF Roles with Skills & Knowledge"),
    "nice_framework": (subgraph_nice_framework, "NICE Framework – Categories & Work Roles"),
    "universities":   (subgraph_universities,   "European Universities, Programs & Countries"),
    "jrc_taxonomy":   (subgraph_jrc_taxonomy,   "JRC Cybersecurity Taxonomy – Domains & Concepts"),
    "role_program":   (subgraph_role_program,    "Role–Program Alignment (ECSF ↔ University Programs)"),
}


def _edge_style(etype: str) -> tuple[str, float, str | None]:
    """Return (color, linewidth, dash-style) for Matplotlib."""
    cfg = EDGE_CONFIG.get(etype, _DEFAULT_EDGE)
    color = cfg["color"]
    width = cfg["width"]
    dash = cfg["dash"]
    style: str | None = None
    if dash:
        style = "dashed"
    return color, width, style


def render_subgraph(
    G: nx.DiGraph,
    title: str,
    out_stem: Path,
    *,
    figsize: tuple[int, int] = (32, 22),
    dpi: int = 200,
    bg: str = "#0d1117",
    label_all: bool = True,
    font_size_range: tuple[int, int] = (7, 14),
) -> None:
    """Render a subgraph to ``<out_stem>.png`` and ``<out_stem>.svg``."""
    if G.number_of_nodes() == 0:
        print(f"  ⚠  Skipping empty subgraph: {title}")
        return

    out_stem.parent.mkdir(parents=True, exist_ok=True)

    # Use  k  parameter to push nodes apart.  Larger k → more spread.
    n = max(G.number_of_nodes(), 2)
    k = 3.5 / (n ** 0.35)          # generous spacing factor
    pos = nx.spring_layout(G, seed=42, iterations=800, k=k, scale=1.0)

    # Normalise into [margin, 1-margin] so labels never clip
    margin = 0.06
    xs = [p[0] for p in pos.values()]
    ys = [p[1] for p in pos.values()]
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi - lo == 0:
            return 0.5
        return margin + (1 - 2 * margin) * ((v - lo) / (hi - lo))

    pos = {node: (_norm(x, xmin, xmax), _norm(y, ymin, ymax)) for node, (x, y) in pos.items()}

    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

   # Group edges by type so we can batch-draw with consistent style
    edge_groups: dict[str, list[tuple[str, str]]] = {}
    for u, v, eattr in G.edges(data=True):
        et = eattr.get("edge_type", "unknown")
        edge_groups.setdefault(et, []).append((u, v))

    for etype, edgelist in edge_groups.items():
        ecolor, ewidth, estyle = _edge_style(etype)
        nx.draw_networkx_edges(
            G, pos, edgelist=edgelist, ax=ax,
            edge_color=ecolor,
            width=ewidth * 1.2,
            style=estyle or "solid",
            alpha=0.45,
            arrows=True,
            arrowsize=8,
            arrowstyle="-|>",
            connectionstyle="arc3,rad=0.05",
            min_source_margin=8,
            min_target_margin=8,
        )

    node_types_present = sorted({a.get("node_type", "other") for _, a in G.nodes(data=True)})
    for ntype in node_types_present:
        nodelist = [n for n, a in G.nodes(data=True) if a.get("node_type") == ntype]
        base_size = _MPL_SIZE.get(ntype, 600)
        sizes = [base_size + G.degree(n) * 25 for n in nodelist]
        color = _MPL_PALETTE.get(ntype, "#94a3b8")
        nx.draw_networkx_nodes(
            G, pos, nodelist=nodelist, ax=ax,
            node_size=sizes,
            node_color=color,
            alpha=0.92,
            linewidths=1.2,
            edgecolors="white",
        )

    fmin, fmax = font_size_range
    max_degree = max((G.degree(n) for n in G.nodes()), default=1) or 1

    labels: dict[str, str] = {}
    font_sizes: dict[str, int] = {}
    for n, a in G.nodes(data=True):
        ntype = a.get("node_type", "other")
        lbl = a.get("label", n)
        # Always label structural / large nodes; optionally label all
        if label_all or ntype in {"center", "category", "ecsf_role", "nice_category",
                                   "nice_role", "university", "program", "country",
                                   "jrc_domain"}:
            labels[n] = _short(lbl, 34)
            # Scale font by importance
            deg_frac = G.degree(n) / max_degree
            fs = int(fmin + (fmax - fmin) * deg_frac)
            if ntype == "center":
                fs = fmax + 2
            elif ntype == "category":
                fs = fmax
            font_sizes[n] = fs

    # Draw labels per-font-size group to let Matplotlib handle each size
    by_size: dict[int, dict[str, str]] = {}
    for n, lbl in labels.items():
        sz = font_sizes[n]
        by_size.setdefault(sz, {})[n] = lbl

    for sz, lbl_dict in by_size.items():
        nx.draw_networkx_labels(
            G, pos, labels=lbl_dict, ax=ax,
            font_size=sz,
            font_color="#e2e8f0",
            font_weight="bold",
            font_family="sans-serif",
            verticalalignment="bottom",
        )

    if G.number_of_edges() < 80:
        edge_labels = {}
        for u, v, eattr in G.edges(data=True):
            et = eattr.get("edge_type", "")
            edge_labels[(u, v)] = et.replace("_", " ").title()
        nx.draw_networkx_edge_labels(
            G, pos, edge_labels=edge_labels, ax=ax,
            font_size=5, font_color="#64748b",
            label_pos=0.5,
            bbox=dict(boxstyle="round,pad=0.1", fc=bg, ec="none", alpha=0.7),
        )

    legend_handles = []
    for ntype in node_types_present:
        color = _MPL_PALETTE.get(ntype, "#94a3b8")
        count = sum(1 for _, a in G.nodes(data=True) if a.get("node_type") == ntype)
        label = f"{ntype.replace('_', ' ').title()} ({count})"
        legend_handles.append(mpatches.Patch(color=color, label=label))
    if legend_handles:
        leg = ax.legend(
            handles=legend_handles, loc="upper left",
            fontsize=10, title="Node Types", title_fontsize=12,
            frameon=True, facecolor="#161b22", edgecolor="#30363d",
            labelcolor="#c9d1d9",
        )
        leg.get_title().set_color("#f0f6fc")

    ax.set_title(title, fontsize=20, fontweight="bold", color="#f0f6fc", pad=18)
    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.axis("off")
    fig.tight_layout(pad=1.5)

    png_path = out_stem.with_suffix(".png")
    svg_path = out_stem.with_suffix(".svg")
    fig.savefig(png_path, dpi=dpi, bbox_inches="tight", facecolor=bg, pad_inches=0.3)
    fig.savefig(svg_path, bbox_inches="tight", facecolor=bg, pad_inches=0.3)
    plt.close(fig)
    print(f"  ✓ {png_path.name:40s}  nodes={G.number_of_nodes():>4d}  edges={G.number_of_edges():>5d}")
    print(f"  ✓ {svg_path.name}")


# Main entry-point: convert GraphML and produce all exports
def run(
    input_graphml: str | Path,
    output_dir: str | Path,
    *,
    subgraphs: list[str] | None = None,
) -> None:
    input_graphml = Path(input_graphml)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: GraphML -> D3 JSON
    json_path = output_dir / "kg_d3_data.json"
    print(f"\nInput:  {input_graphml}")
    print(f"Output: {output_dir}/\n")

    data = graphml_to_d3_json(input_graphml, json_path)

    # Step 2: Build NetworkX graph 
    G = _build_nx(data)
    print(f"\n  Full graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")

    # Step 3: Render subgraphs
    targets = subgraphs or list(SUBGRAPH_REGISTRY.keys())
    print("Rendering subgraphs:")

    for name in targets:
        if name not in SUBGRAPH_REGISTRY:
            print(f"  ⚠  Unknown subgraph '{name}', skipping.")
            continue
        extractor, title = SUBGRAPH_REGISTRY[name]
        sg = extractor(G)
        stem = output_dir / f"kg_{name}"
        render_subgraph(sg, title, stem)

    # Step 4: Also render a readable full-graph with reduced density
    print("\nRendering readable full-graph (top nodes per type):")
    full_readable = _readable_full_graph(G)
    render_subgraph(
        full_readable,
        "Cybersecurity Education Knowledge Graph (Readable Subset)",
        output_dir / "kg_full_readable",
        figsize=(40, 28),
        dpi=200,
        label_all=True,
    )

    print(f"\n All exports written to {output_dir}/\n")



"""Keep a manageable subset of the full graph: all structural nodes plus
a capped number of leaf nodes so the figure stays readable"""
def _readable_full_graph(G: nx.DiGraph) -> nx.DiGraph:
    keep: set[str] = set()
    # Always keep structural nodes
    keep |= _nodes_of_type(G, "center", "category", "ecsf_role", "nice_category", "country")

    per_type_cap = {
        "nice_role": 15,
        "university": 20,
        "program": 20,
        "skill": 12,
        "knowledge": 10,
        "jrc_domain": 15,
        "jrc_concept": 10,
    }
    for ntype, cap in per_type_cap.items():
        keep.update(_top_by_degree(G, ntype, cap))

    return G.subgraph(keep).copy()


# Argument parser for standalone invocation
def cli() -> None:
    BASE = Path(__file__).resolve().parent.parent.parent  # thesis_project/

    parser = argparse.ArgumentParser(
        prog="convert_graphml_to_d3",
        description="Convert a GraphML knowledge graph to D3 JSON + PNG/SVG subgraph exports.",
    )
    parser.add_argument(
        "--input", "-i",
        default=str(BASE / "pipeline_output" / "cybersecurity_education_kg_v2.graphml"),
        help="Path to the input GraphML file (default: pipeline_output/cybersecurity_education_kg_v2.graphml)",
    )
    parser.add_argument(
        "--outdir", "-o",
        default=str(BASE / "pipeline_output" / "graph_figures"),
        help="Output directory for JSON + images (default: pipeline_output/graph_figures/)",
    )
    parser.add_argument(
        "--subgraphs", "-s", nargs="*",
        choices=list(SUBGRAPH_REGISTRY.keys()),
        help="Render only specific subgraphs (default: all).",
    )
    args = parser.parse_args()
    run(args.input, args.outdir, json_only=False, subgraphs=args.subgraphs)


if __name__ == "__main__":
    cli()
