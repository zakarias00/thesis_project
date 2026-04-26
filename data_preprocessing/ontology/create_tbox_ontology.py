import xml.dom.minidom as minidom
import os
import pathlib
from xml.etree.ElementTree import Element, SubElement, tostring

# Generates TBox ontology for the courses dataset schema
def generate_tbox(output_path="tbox.owl"):
    rdf = Element(
        "rdf:RDF",
        {
            "xmlns:rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "xmlns:rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "xmlns:owl": "http://www.w3.org/2002/07/owl#",
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema#",
            "xml:base": "http://example.org/course_ontology",
        },
    )

    # Ontology root
    SubElement(rdf, "owl:Ontology", {"rdf:about": "http://example.org/course_ontology"})

    # Classes
    classes = ["Course", "Skill", "DescriptionText"]
    for cls in classes:
        SubElement(rdf, "owl:Class", {"rdf:about": f"#{cls}"})

    # Object properties
    object_props = {
        "hasSkill": ("Course", "Skill"),
        "hasDescription": ("Course", "DescriptionText"),
    }
    for prop, (dom, ran) in object_props.items():
        p = SubElement(rdf, "owl:ObjectProperty", {"rdf:about": f"#{prop}"})
        SubElement(p, "rdfs:domain", {"rdf:resource": f"#{dom}"})
        SubElement(p, "rdfs:range", {"rdf:resource": f"#{ran}"})

    # Data properties
    data_props = {
        "courseTitle": "Course",
        "descriptionText": "DescriptionText",
        "originalDescription": "DescriptionText",
        "combinedDescription": "DescriptionText",
        "skillName": "Skill",
    }
    for prop, dom in data_props.items():
        p = SubElement(rdf, "owl:DatatypeProperty", {"rdf:about": f"#{prop}"})
        SubElement(p, "rdfs:domain", {"rdf:resource": f"#{dom}"})
        SubElement(p, "rdfs:range", {"rdf:resource": "xsd:string"})

    xml_str = minidom.parseString(tostring(rdf)).toprettyxml(indent="  ")

    actual_output_path = os.path.abspath(pathlib.Path(output_path))

    with open(actual_output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)

    print(f"TBox ontology saved to {actual_output_path}")


if __name__ == "__main__":
    generate_tbox("tbox.owl")
