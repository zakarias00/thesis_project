# ECSF Cybersecurity Education Pipeline Report

**Generated:** 2026-04-27T16:42:39.060934

**Pipeline version:** 2.0.0

## Pipeline Summary

- **Elapsed:** 40.6 s
- **Programs analyzed:** 36
- **Framework items:** 470
- **SOA entries:** 7401
- **Stages executed:** ingestion, framework_mapping, lo_extraction, assessment_extraction, soa, breadth, depth, progression, immersion, nlp, ontology, embeddings, scoring, validation, feedback, graph, reporting

## Stages Executed

- [x] ingestion
- [x] framework_mapping
- [x] lo_extraction
- [x] assessment_extraction
- [x] soa
- [x] breadth
- [x] depth
- [x] progression
- [x] immersion
- [x] nlp
- [x] ontology
- [x] embeddings
- [x] scoring
- [x] validation
- [x] feedback
- [x] graph
- [x] reporting

## Top Programs by Composite Score

| Rank | Program | University | Score | Grade |
|------|---------|-----------|-------|-------|
| 1 | Computer Engineering, Cybersecurity and Artificial Intelligence | University of Cagliari | 0.801 | A |
| 2 | MSc Cybersecurity | University College Dublin | 0.793 | B |
| 3 | Cybersecurity (Engineering) | Politcnico di Torino | 0.775 | B |
| 4 | Cybersecurity | KTH Royal Institute of Technology | 0.774 | B |
| 5 | Digital Infrastructure and Cyber Security | Norwegian University of Science and Technology (NTNU) | 0.763 | B |
| 6 | Master's Degree in Cybersecurity and Data Intelligence | Universidad de La Laguna | 0.754 | B |
| 7 | Master of Research in Cybersecurity | Universidad de León | 0.749 | B |
| 8 | Master in Cybersecurity | University of Aveiro | 0.747 | B |
| 9 | Cybersecurity and Data Science | University of Piraeus | 0.743 | B |
| 10 | Master of Computer Systems, Communication and Security (Information Security Specialization) | Masaryk University | 0.737 | B |
| 11 | Computer engineering - second degree study, specialization cybersecurity and cloud technologies | Rzeszów University of Technology | 0.724 | B |
| 12 | Information Security Management | FH OÖ | 0.722 | B |
| 13 | Master's study programme Cyber Security | Faculty of Information Studies in Novo mesto | 0.721 | B |
| 14 | Master of Electrical Engineering (ICT Security and Networks) | KU Leuven | 0.716 | B |
| 15 | Networking, Infrastructure and Cybersecurity (NICS) | Jönköping University | 0.709 | B |
| 16 | Advanced Master of Cybersecurity | KU Leuven | 0.707 | B |
| 17 | University Master in Cybersecurity | University of Alcala | 0.706 | B |
| 18 | Digital Technologies and Information Security | Iscte - Instituto Universitário de Lisboa | 0.699 | B |
| 19 | Information and Information Technologies Security | Vilnius Gedmininas Technical University (VilniusTech) | 0.689 | B |
| 20 | Master in Engineering, Cybersecurity | South-Eastern Finland University of Applied Sciences | 0.683 | B |

## Score Distribution

- **mean:** 0.6591
- **std:** 0.1153
- **min:** 0.3214
- **25%:** 0.6253
- **50%:** 0.6942
- **75%:** 0.7380
- **max:** 0.8010

## Validation Summary

- **Coverage improvement (regex → full):** +0.2510
- **Spearman ρ:** 0.6107
- **Top-N stability:** 0.4667

### Ablation Results

| Method | Mean Coverage | Std |
|--------|--------------|-----|
| regex_only | 0.3756 | 0.1244 |
| regex_nlp | 0.4037 | 0.1492 |
| regex_nlp_ontology | 0.5791 | 0.1223 |
| full | 0.6266 | 0.0869 |

### Framework Method Comparison (Jaccard)

| Method A | Method B | Mean Jaccard | Median | Std |
|----------|----------|-------------|--------|-----|
| ecsf | nice | 0.4335 | 0.4310 | 0.1151 |
| ecsf | jrc | 0.4755 | 0.4615 | 0.0982 |
| nice | jrc | 0.5019 | 0.5000 | 0.1139 |

---