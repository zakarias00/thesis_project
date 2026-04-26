import xml.dom.minidom as minidom
from xml.etree.ElementTree import Element, SubElement, tostring
import pandas as pd
import os
import pathlib

# Generates ABox ontology instances from the courses dataset

# Default column mapping: canonical name -> possible raw CSV names
DEFAULT_COLUMN_MAP = {
    "course_title": ["course_title", "title", "course title"],
    "description": ["description", "Description", "study program description"],
    "original_description": ["original_description", "original description"],
    "combined_description": ["combined_description", "combined description"],
    "extracted_skills": ["extracted_skills", "extracted skills"],
}


def _resolve_column(df, canonical_name, column_map=None):
    """Find the actual column name in df, trying the column_map first."""
    if column_map and canonical_name in column_map:
        candidate = column_map[canonical_name]
        if candidate in df.columns:
            return candidate
    for candidate in DEFAULT_COLUMN_MAP.get(canonical_name, [canonical_name]):
        if candidate in df.columns:
            return candidate
    return None


def generate_abox(csv_path, output_path="abox.owl", column_map=None):
    df = pd.read_csv(csv_path)

    # Resolve columns via schema-tolerant lookup
    col_title = _resolve_column(df, "course_title", column_map)
    col_desc = _resolve_column(df, "description", column_map)
    col_orig = _resolve_column(df, "original_description", column_map)
    col_comb = _resolve_column(df, "combined_description", column_map)
    col_skills = _resolve_column(df, "extracted_skills", column_map)

    if col_title is None:
        raise ValueError(f"Cannot find course_title column. Available: {list(df.columns)}")

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

    # ABox: Instances for each dataset row
    for idx, row in df.iterrows():
        # Course individual
        title_val = str(row[col_title]) if col_title else f"course_{idx}"
        cid = title_val.replace(" ", "_")
        course = SubElement(rdf, "owl:NamedIndividual", {"rdf:about": f"#{cid}"})
        SubElement(course, "rdf:type", {"rdf:resource": "#Course"})
        SubElement(course, "courseTitle", {"rdf:datatype": "xsd:string"}).text = title_val

        # Description object
        desc_id = f"Desc_{cid}"
        desc = SubElement(rdf, "owl:NamedIndividual", {"rdf:about": f"#{desc_id}"})
        SubElement(desc, "rdf:type", {"rdf:resource": "#DescriptionText"})
        SubElement(desc, "descriptionText", {"rdf:datatype": "xsd:string"}).text = (
            str(row[col_desc]) if col_desc and pd.notna(row.get(col_desc)) else ""
        )
        SubElement(desc, "originalDescription", {"rdf:datatype": "xsd:string"}).text = (
            str(row[col_orig]) if col_orig and pd.notna(row.get(col_orig)) else ""
        )
        SubElement(desc, "combinedDescription", {"rdf:datatype": "xsd:string"}).text = (
            str(row[col_comb]) if col_comb and pd.notna(row.get(col_comb)) else ""
        )

        # Link description to course
        SubElement(course, "hasDescription", {"rdf:resource": f"#{desc_id}"})

        # Skills
        skills_val = row.get(col_skills) if col_skills else None
        if skills_val is not None and pd.notna(skills_val):
            skills = [s.strip() for s in str(skills_val).split(',')]
            for s in skills:
                sid = s.replace(" ", "_")
                skill = SubElement(rdf, "owl:NamedIndividual", {"rdf:about": f"#{sid}"})
                SubElement(skill, "rdf:type", {"rdf:resource": "#Skill"})
                SubElement(skill, "skillName", {"rdf:datatype": "xsd:string"}).text = s
                SubElement(course, "hasSkill", {"rdf:resource": f"#{sid}"})

    xml_str = minidom.parseString(tostring(rdf)).toprettyxml(indent=" ")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)

    print(f"ABox ontology saved to {output_path}")

if __name__ == "__main__":
    actual_output_path = os.path.abspath(pathlib.Path("abox.owl"))
    dataset_csv_path = os.path.abspath(pathlib.Path("courses_dataset.csv"))
    generate_abox(dataset_csv_path, actual_output_path)