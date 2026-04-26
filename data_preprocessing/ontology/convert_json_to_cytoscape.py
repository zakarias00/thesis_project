import json

with open("ontology.json") as f:
    data = json.load(f)

graph = data["graphs"][0]

nodes_out = []
edges_out = []

# 1. Convert nodes
for n in graph["nodes"]:
    nodes_out.append({
        "data": {
            "id": n["id"],
            "label": n["id"].split("#")[-1],
            "type": n["type"],
            "propertyType": n.get("propertyType", None)
        }
    })

# 2. Convert domain-range axioms to edges
for ax in graph["domainRangeAxioms"]:
    predicate = ax["predicateId"]
    domains = ax["domainClassIds"]
    ranges = ax["rangeClassIds"]

    for d in domains:
        for r in ranges:
            edges_out.append({
                "data": {
                    "id": predicate + "_edge",
                    "source": d,
                    "target": r,
                    "label": predicate.split("#")[-1]
                }
            })

output = {
    "elements": {
        "nodes": nodes_out,
        "edges": edges_out
    }
}

with open("cytoscape_ontology.json", "w") as f:
    json.dump(output, f, indent=2)
