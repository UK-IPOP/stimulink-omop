import argparse
import datetime
from itertools import zip_longest
import json
import multiprocessing
import subprocess
from pathlib import Path
import time
from typing import Any, Iterator

import pandas as pd
from rich.console import Console
from p_tqdm import p_uimap
from nltk.tokenize import sent_tokenize
import nltk
import random
import orjson

from functools import partial
from pathlib import Path
import pandas as pd
from rich import print
import spacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
from p_tqdm import p_uimap
from scispacy.candidate_generation import DEFAULT_PATHS, DEFAULT_KNOWLEDGE_BASES
from scispacy.candidate_generation import CandidateGenerator, LinkerPaths
from scispacy.linking_utils import KnowledgeBase, Entity as LinkedEntity
import spacy
import scispacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
from scispacy.linking_utils import KnowledgeBase
from scispacy.linking import EntityLinker
from scispacy.candidate_generation import CandidateGenerator, LinkerPaths
from scispacy.candidate_generation import DEFAULT_PATHS, DEFAULT_KNOWLEDGE_BASES
from pathlib import Path
from rich import print
import polars as pl
from tqdm import tqdm


print("Downloading `punkt` sentence tokenizer...")
nltk.download("punkt")

c = Console()

# TODO: add negation
# https://github.com/jenojp/negspacy
# TODO: use spacy batching
# TODO: research spacy speed improvements with nlp.pipe


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


def setup_paths():
    """Sets up the output paths for the script."""
    p = Path() / "data" / "scispacy_output"
    p.mkdir(exist_ok=True, parents=True)

    p = Path() / "data" / "scispacy_output" / "batches"
    p.mkdir(exist_ok=True, parents=True)


def load_data(fpath: str) -> pl.DataFrame:
    """Loads the data from file provided.

    EXPECTS FEATHER FILE

    Returns:
        Data frame of notes with only ID and text columns
    """
    # manually reset index for now
    # 97 nulls apparently in text as of today (3-29-2023)
    df = pl.scan_ipc(fpath).select(["note_id", "NOTE_TEXT"]).drop_nulls().collect()
    return df


def process_row(row: tuple[int, str] | None) -> list[dict[str, Any]] | None:
    if row is None:
        return None
    id_, text = row
    results: list[dict[str, Any]] = []
    # run metamap
    doc = nlp(text)
    for ent in doc.ents:
        for kb_ent in ent._.kb_ents:
            concept: LinkedEntity = linker.kb.cui_to_entity[kb_ent[0]]
            # definition = (
            #     concept.definition.strip() if concept.definition is not None else None
            # )
            # i decided to NOT make this match OMOP but instead pull what could only be determined at this runtime
            # and we can later add extra fields where OMOP wants
            data = {
                "row_num": id_,
                "cui": concept.concept_id.strip(),
                # "name": concept.canonical_name.strip(),
                "score": kb_ent[1],
                "entity": ent.text.strip(),
                # "definition": definition,
                # "tui": "-".join(concept.types).strip(),
                "nlp_datetime": datetime.datetime.now().isoformat(),
                # we can use this and something like rust's `.position` to find a few chars around the string if these
                # sentences are too long... this will map to OMOP `snippet` field
                # "sentence": text,
            }
            results.append(data)
    return results


def punk_it(row: tuple[int, str]) -> list[tuple[int, str]]:
    results = []
    for sent in sent_tokenize(row[1]):
        results.append((row[0], sent))
    return results


def compress_output(batch: int, in_path: Path):
    """Compresses the output file."""
    c.log(f"[yellow]Compressing chunk:...")
    out_path = Path() / "data" / "scispacy_output" / "batches" / f"{batch}.feather"
    pl.scan_ndjson(in_path).collect().write_ipc(out_path, compression="lz4")
    # clear the output file
    with open(in_path, "r+") as f:
        f.truncate(0)


def run(fpath: str) -> None:
    """Runs the script.

    Args:
        fpath (str): The path to the dataset.
        text_col (str): The text column to run Scispacy on.
        id_col (str): The identifier column
    """
    setup_paths()
    c.log("Loading data...")
    df = load_data(fpath=fpath)

    c.log("Dicing sentences...")

    # this explodes the data to multiple rows per note
    # where the new rows/note is the # of sentences in the note
    # rows = df.head(1000000).to_numpy()
    rows = df.to_numpy()
    sentence_data = []
    # for id_, text in tqdm(rows):
    #     sentence_data.append((id_, text))
    # for sent in sent_tokenize(text):
    #     sentence_data.append((id_, sent))

    chunk_size = 100_000
    sentence_chunks = list(zip_longest(*[iter(rows)] * chunk_size))
    # sentence_chunks = list(zip_longest(*[iter(sentence_data)] * chunk_size))
    num_chunks = len(sentence_chunks)
    print(f"There are {num_chunks} chunks to process...")
    del df, rows, sentence_data

    c.log("NLP-ing sentences...")
    temp_path = Path("data") / "scispacy_output" / "temp.jsonl"
    for j, chunk in enumerate(sentence_chunks):
        i = j + 1
        c.log(f"[blue]Processing chunk {i}/{num_chunks}")
        # already done
        # if i < 133:
        # continue
        with open(temp_path, "wb") as f:
            for result in p_uimap(process_row, chunk, num_cpus=48):
                if result is None:
                    continue
                for line in result:
                    f.write(orjson.dumps(line) + b"\n")
        c.log(f"[green]Finished chunk: {i}/{num_chunks}...")

        # take output, collect it, and write it to a feather file
        # then clear the output
        # this is to help with storage and memory
        compress_output(batch=i, in_path=temp_path)

    c.log("[green]Done!")


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
    # run(fpath="data/notes.feather")
    run(fpath=args.file)
