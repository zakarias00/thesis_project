import argparse
import os
import re
import textwrap
from collections import Counter
from typing import List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from utils.io_functions import ensure_outdir, write_markdown, savefig
from utils.eda_functions import clean_colnames, is_nonempty_str, get_present_columns
from utils.text_mining_functions import word_tokenize, remove_stopwords, top_n, bigrams, parse_skills_cell, summarize_text_lengths
from utils.visuzalization_functions import save_barplot

# ---------------------------
# Configuration
# ---------------------------

# stopword list
STOPWORDS = {
    "a","an","and","are","as","at","be","by","for","from","has","he","in","is","it","its",
    "of","on","that","the","to","was","were","will","with","this","these","those","or",
    "we","you","your","their","they","them","our","us","i","me","my","mine","hers","his",
    "her","itself","him","himself","herself","ourselves","yourselves","themselves",
    "but","not","no","so","if","than","then","also","can","could","should","would","may",
    "might","about","into","over","under","between","after","before","during","out","up",
    "down","again","further","once","because","until","while","both","each","few","more",
    "most","other","some","such","only","own","same","too","very","s","t","just","don",
    "now"
}

DEFAULT_TEXT_COLUMNS_CANDIDATES = [
    "course_title",
    "title",
    "description",
    "original_description",
    "combined_description",
    "extracted_skills",
]
PLOT_STYLE = "whitegrid"
RANDOM_STATE = 42

# ---------------------------
# EDA pipeline
# ---------------------------

def eda_courses(input_csv: str, outdir: str, show: bool = False) -> None:
    sns.set_style(PLOT_STYLE)
    np.random.seed(RANDOM_STATE)

    ensure_outdir(outdir)
    report_lines = []

    # Load dataset
    df = pd.read_csv(input_csv, engine="python", encoding="utf-8", on_bad_lines="skip")
    orig_shape = df.shape
    df = clean_colnames(df)

    report_lines.append(f"# EDA Report for {os.path.basename(input_csv)}")
    report_lines.append("")
    report_lines.append(f"- Rows: {orig_shape[0]}")
    report_lines.append(f"- Columns: {orig_shape[1]}")
    report_lines.append("")
    report_lines.append("## Columns and dtypes")
    for c in df.columns:
        report_lines.append(f"- {c}: {df[c].dtype}")
    report_lines.append("")

    # Missing values
    miss = df.isna().sum().sort_values(ascending=False)
    mv = miss.reset_index()
    mv.columns = ["column", "missing_count"]
    mv["missing_pct"] = (mv["missing_count"] / len(df) * 100).round(2)
    mv.to_csv(os.path.join(outdir, "missing_values.csv"), index=False)

    if len(mv):
        plt.figure(figsize=(10, 6))
        sns.barplot(data=mv, x="column", y="missing_count", color="#d62728")
        plt.title("Missing Values by Column")
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("Missing Count")
        plt.xlabel("Column")
        savefig(os.path.join(outdir, "missing_values.png"))
        report_lines.append("## Missing values")
        report_lines.append("")
        report_lines.append("See: missing_values.csv and missing_values.png")
        report_lines.append("")

    # Duplicates
    dup_total = int(df.duplicated().sum())
    dup_title = None
    title_col = "course_title" if "course_title" in df.columns else ("title" if "title" in df.columns else None)
    if title_col:
        dup_title = int(df[title_col].duplicated().sum())

    report_lines.append("## Duplicates")
    report_lines.append(f"- Total duplicate rows: {dup_total}")
    if dup_title is not None:
        report_lines.append(f"- Duplicate {title_col} values: {dup_title}")
    report_lines.append("")

    # Text columns present
    text_cols = get_present_columns(df, DEFAULT_TEXT_COLUMNS_CANDIDATES)
    report_lines.append("## Text columns detected")
    if text_cols:
        for c in text_cols:
            report_lines.append(f"- {c}")
    else:
        report_lines.append("- None detected from standard candidates")
    report_lines.append("")

    # Text length summaries and plots
    for c in text_cols:
        if c == "extracted_skills":
            continue
        desc_df = summarize_text_lengths(df[c])
        desc_df.to_csv(os.path.join(outdir, f"{c}_lengths.csv"), index=False)

        # Histograms
        for col, title_part in [
            ("char_len", "Character Length"),
            ("word_count", "Word Count"),
            ("sentence_count", "Sentence Count"),
        ]:
            plt.figure(figsize=(10, 6))
            sns.histplot(desc_df[col], kde=True, bins=40, color="#1f77b4")
            plt.title(f"{title_part} Distribution for {c}")
            plt.xlabel(title_part)
            plt.ylabel("Frequency")
            savefig(os.path.join(outdir, f"{c}_{col}_hist.png"))

    report_lines.append("## Text length outputs")
    report_lines.append("Generated CSVs and histograms for detected text columns (character length, word count, sentence count).")
    report_lines.append("")

    # Token frequency analysis on a preferred description field
    preferred_desc = None
    for candidate in ["combined_description", "description", "original_description"]:
        if candidate in df.columns:
            preferred_desc = candidate
            break

    if preferred_desc is not None:
        report_lines.append(f"## Token analysis on: {preferred_desc}")
        texts = df[preferred_desc].fillna("").astype(str).tolist()
        tokens_all: List[str] = []
        for t in texts:
            toks = word_tokenize(t, min_len=2)
            toks = remove_stopwords(toks, STOPWORDS)
            tokens_all.extend(toks)

        token_counts = Counter(tokens_all)
        token_df = pd.DataFrame(top_n(token_counts, 100), columns=["token", "count"])
        token_df.to_csv(os.path.join(outdir, "top_tokens.csv"), index=False)
        if len(token_df):
            save_barplot(
                token_df, x="token", y="count",
                title=f"Top Tokens in {preferred_desc}",
                outfile=os.path.join(outdir, "top_tokens.png"),
                top_n=30, rotate=65
            )

        # Bigrams
        bigram_counter = Counter()
        for t in texts:
            toks = remove_stopwords(word_tokenize(t, min_len=2), STOPWORDS)
            bigram_counter.update([" ".join(bg) for bg in bigrams(toks)])
        bigram_df = pd.DataFrame(top_n(bigram_counter, 100), columns=["bigram", "count"])
        bigram_df.to_csv(os.path.join(outdir, "top_bigrams.csv"), index=False)
        if len(bigram_df):
            save_barplot(
                bigram_df, x="bigram", y="count",
                title=f"Top Bigrams in {preferred_desc}",
                outfile=os.path.join(outdir, "top_bigrams.png"),
                top_n=30, rotate=65
            )
        report_lines.append("- Generated token and bigram frequency plots and CSVs.")
        report_lines.append("")
    else:
        report_lines.append("## Token analysis")
        report_lines.append("- No description-like column found for token analysis.")
        report_lines.append("")

    # Extracted skills analysis
    if "extracted_skills" in df.columns:
        report_lines.append("## Extracted skills analysis")
        all_skills: List[str] = []
        for cell in df["extracted_skills"]:
            skills = parse_skills_cell(cell)
            # Normalize skills
            norm = []
            for s in skills:
                s_clean = re.sub(r"\s+", " ", s).strip().lower()
                if s_clean:
                    norm.append(s_clean)
            all_skills.extend(norm)

        skill_counts = Counter(all_skills)
        if skill_counts:
            skills_df = pd.DataFrame(skill_counts.most_common(100), columns=["skill", "count"])
            skills_df.to_csv(os.path.join(outdir, "top_skills.csv"), index=False)
            save_barplot(
                skills_df, x="skill", y="count",
                title="Top Extracted Skills",
                outfile=os.path.join(outdir, "top_skills.png"),
                top_n=30, rotate=65
            )
            report_lines.append("- Generated top skills CSV and bar plot.")
        else:
            report_lines.append("- No skills detected after parsing.")
        report_lines.append("")
    else:
        report_lines.append("## Extracted skills analysis")
        report_lines.append("- Column 'extracted_skills' not found.")
        report_lines.append("")

    # Basic per-column cardinality and sample values
    report_lines.append("## Cardinality and sample values")
    card_rows = []
    for c in df.columns:
        nunique = int(df[c].nunique(dropna=True))
        sample_vals = [repr(x) for x in df[c].dropna().astype(str).head(3).tolist()]
        card_rows.append((c, nunique, "; ".join(sample_vals)))
    card_df = pd.DataFrame(card_rows, columns=["column", "unique_values", "sample_values"])
    card_df.to_csv(os.path.join(outdir, "cardinality_samples.csv"), index=False)
    report_lines.append("- See cardinality_samples.csv")
    report_lines.append("")

    # Numeric columns summary
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if numeric_cols:
        desc = df[numeric_cols].describe().T
        desc.to_csv(os.path.join(outdir, "numeric_summary.csv"))
        plt.figure(figsize=(10, 6))
        sns.heatmap(df[numeric_cols].corr(numeric_only=True), annot=True, fmt=".2f", cmap="coolwarm", square=False)
        plt.title("Correlation Heatmap (Numeric Columns)")
        savefig(os.path.join(outdir, "correlation_heatmap.png"))
        report_lines.append("## Numeric columns")
        report_lines.append("- Generated numeric_summary.csv and correlation_heatmap.png")
        report_lines.append("")
    else:
        report_lines.append("## Numeric columns")
        report_lines.append("- No numeric columns detected.")
        report_lines.append("")

    # Save report
    report_md = "\n".join(report_lines)
    write_markdown(os.path.join(outdir, "EDA_REPORT.md"), report_md)

    if show:
        print(textwrap.dedent(report_md))
        print(f"\nArtifacts saved to: {os.path.abspath(outdir)}")


def main():
    parser = argparse.ArgumentParser(description="Exploratory Data Analysis for courses dataset")
    parser.add_argument("--input", "-i", type=str, default="courses_dataset.csv", help="Path to the input CSV file")
    parser.add_argument("--outdir", "-o", type=str, default="eda_output", help="Directory to store outputs")
    parser.add_argument("--show", action="store_true", help="Print report to console after finishing")
    args = parser.parse_args()

    eda_courses(input_csv=args.input, outdir=args.outdir, show=args.show)


if __name__ == "__main__":
    main()