import argparse
import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
from rich.console import Console
from p_tqdm import p_uimap
from nltk.tokenize import sent_tokenize
import nltk
import random
import orjson

from pathlib import Path
import pandas as pd
from rich import print
from scispacy.linking_utils import KnowledgeBase, Entity as LinkedEntity
import spacy
import scispacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
from rich import print
import polars as pl
from tqdm import tqdm


print("Downloading `punkt` sentence tokenizer...")
nltk.download("punkt")

c = Console()

# SNOMED_LINKER_PATHS = LinkerPaths(
#     ann_index="./data/models/nmslib_index.bin",
#     tfidf_vectorizer="./data/models/tfidf_vectorizer.joblib",
#     tfidf_vectors="./data/models/tfidf_vectors_sparse.npz",
#     concept_aliases_list="./data/models/concept_aliases.json",
# )


# class SnomedKnowledgeBase(KnowledgeBase):
#     def __init__(
#         self,
#         file_path: str = "./data/knowledge_bases/snomed.jsonl",
#     ):
#         super().__init__(file_path)


# # Admittedly this is a bit of a hack, because we are mutating a global object.
# # However, it's just a kind of registry, so maybe it's ok.
# DEFAULT_PATHS["snomed"] = SNOMED_LINKER_PATHS
# DEFAULT_KNOWLEDGE_BASES["snomed"] = SnomedKnowledgeBase


print("Initializing Scispacy...")

nlp = spacy.load("en_core_sci_lg")

# nlp.add_pipe("abbreviation_detector")
nlp.add_pipe(
    "scispacy_linker",
    config={
        "resolve_abbreviations": True,
        "max_entities_per_mention": 3,
        "k": 30,  # default is 30
        "threshold": 0.9,  # default is 0.7
        "filter_for_definitions": True,  # this is the big one! limits to only entities with definitions in knowledge base, oh was already true by default
        "linker_name": "umls",
    },
)
linker = nlp.get_pipe("scispacy_linker")


def load_data(fpath: str) -> pl.DataFrame:
    df = pl.read_csv(fpath)
    print(f"Initial shape: {df.shape}")
    df = df.select(["CASENUMBER", "INCIDENT NARRATIVE"]).drop_nulls()
    print(f"Post-drop nulls shape: {df.shape}")
    return df


def process_row(text: str, id_: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    # run metamap
    doc = nlp(text)
    for ent in doc.ents:
        for kb_ent in ent._.kb_ents:
            concept: LinkedEntity = linker.kb.cui_to_entity[kb_ent[0]]
            definition = (
                concept.definition.strip() if concept.definition is not None else None
            )
            # i decided to NOT make this match OMOP but instead pull what could only be determined at this runtime
            # and we can later add extra fields where OMOP wants
            data = {
                "row_num": id_,
                "cui": concept.concept_id.strip(),
                "name": concept.canonical_name.strip(),
                "score": kb_ent[1],
                "entity": ent.text.strip(),
                "definition": definition,
                "tui": "-".join(concept.types).strip(),
                "nlp_datetime": datetime.datetime.now().isoformat(),
                # we can use this and something like rust's `.position` to find a few chars around the string if these
                # sentences are too long... this will map to OMOP `snippet` field
            }
            results.append(data)
    return results


def run(fpath: str) -> None:
    """Runs the script.

    Args:
        fpath (str): The path to the dataset.
        text_col (str): The text column to run Scispacy on.
        id_col (str): The identifier column
    """
    c.log("Loading data...")
    df = load_data(fpath=fpath)

    c.log("Dicing sentences...")

    # this explodes the data to multiple rows per note
    # where the new rows/note is the # of sentences in the note
    rows = df.to_dicts()

    c.log("NLP-ing sentences...")
    with open("cook_county_output.jsonl", "wb") as f:
        for row in tqdm(rows):
            for result in process_row(
                text=row["INCIDENT NARRATIVE"], id_=row["CASENUMBER"]
            ):
                if len(result) == 0:
                    continue
                f.write(orjson.dumps(result) + b"\n")
    c.log("[green]Done nlp-ing!")


def analyze():
    df = pl.read_ndjson("cook_county_output.jsonl")
    print(f"Output file shape: {df.shape}")
    print(f"Total Unique CUIs: {df.select('cui').unique().shape}")
    homeless_cuis = [
        "C0019863",  # homeless persons
        "C0237154",  # homelessness
        "C0425241",  # homeless family
        "C0681146",  # homeless shelter
        "C0425242",  # homeless single person
        "C5699932",  # history of homeless
        "C2184138",  # living in homeless shelter
        "C0337630",  # housing problem
    ]
    homeless_df = df.filter(pl.col("cui").is_in(homeless_cuis))
    print(f"Homeless results: {homeless_df.shape}")
    print(f"Unique homeless CUIs: {homeless_df.select('cui').unique().shape}")
    print(f"Unique homeless names: {homeless_df.select('name').unique()}")
    homeless_df.to_pandas().to_csv("homeless.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="scispacy_notes",
        description="Runs Scispacy on the text in the provided dataset.",
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        help="The file to run Scispacy on. Must be a feather file.",
    )
    args = parser.parse_args()
    run(fpath=args.file)
    analyze()
