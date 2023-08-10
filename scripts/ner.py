# builtins
import argparse
import datetime
from typing import Iterator
from pathlib import Path

# logging/printing
from rich.console import Console
from rich.progress import track

# dataframes
import polars as pl

# spacy libraries
import spacy
from spacy.language import Language
from spacy.tokens import Doc
import scispacy

# spacy pipeline components
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
from scispacy.linking_utils import Entity
from negspacy.negation import Negex

# ignore warnings
import warnings

from tqdm import tqdm

warnings.filterwarnings("ignore")

# initialize logger
logger = Console()

# GLOBAL configs
N_PROCESSES = 20  # ! CAUTION
BATCH_SIZE = 100  # default is 1k
TOTAL = 0
FILE_BATCH_SIZE = 50_000  # number of NLP results to write to file
DT = datetime.datetime.now().isoformat()

if not Doc.has_extension("row_id"):
    Doc.set_extension("row_id", default=None)


def setup_paths():
    """Sets up the output paths for the script."""
    p = Path().cwd().parent / "data" / "ner_output"
    p.mkdir(exist_ok=True, parents=True)

    p = Path().cwd().parent / "data" / "ner_output" / "batches"
    p.mkdir(exist_ok=True, parents=True)


def setup() -> tuple[Language, EntityLinker]:
    logger.log("[cyan]Setting up...")
    setup_paths()

    # load scispacy large biomedical NER model
    # disable everything except the NER model
    logger.log("[yellow]Loading spacy model...")
    nlp = spacy.load(
        "en_core_sci_lg",
        exclude=["tagger", "lemmatizer", "textcat"],
    )

    # load spacy pipeline components
    logger.log("[yellow]Loading spacy pipeline components...")
    nlp.add_pipe("negex")
    nlp.add_pipe("abbreviation_detector", config={"make_serializable": True})
    nlp.add_pipe(
        "scispacy_linker",
        config={
            "resolve_abbreviations": True,
            "max_entities_per_mention": 3,
            "k": 30,  # default is 30
            "threshold": 0.9,  # default is 0.7
            # this is the big one! limits to only entities with definitions in knowledge base, oh was already true by default
            "filter_for_definitions": True,
            "linker_name": "umls",
        },
    )
    # return nlp object and scispacy linker object
    linker: EntityLinker = nlp.get_pipe("scispacy_linker")  # type: ignore
    return nlp, linker


def load_data(
    fpath: Path, text_col: str, id_col: str
) -> list[tuple[str, dict[str, str]]]:
    """Loads the data from file provided.

    EXPECTS FEATHER FILE path

    Args:
        fpath (Path): Path to the file.
        text_col (str): Name of the text column.
        id_col (str): Name of the id column.

    Returns:
        Context data, text records with row_num as additional context
    """
    logger.log("[cyan]Loading data and applying context...")
    data = (
        pl.scan_ipc(fpath)
        .select([text_col, id_col])
        .drop_nulls()
        # remove some extra whitespace around the whole note
        .with_columns([pl.col(text_col).str.strip()])
        # .collect()
        .fetch(1_000_000)
        .to_numpy()
    )
    context_data: list[tuple[str, dict[str, str]]] = []
    for row in data:
        # (text, context_dict)
        context_data.append((row[0], {"row_id": row[1]}))
    # global trickery
    global TOTAL
    TOTAL = len(context_data)
    return context_data


def call_nlp(nlp: Language, text_tuples: list[tuple[str, dict[str, str]]]) -> list[Doc]:
    """Calls the NLP pipeline on the data.

    Args:
        nlp (Language): The spacy language object.
        text_tuples (list[tuple[str, dict[str, str]]]): Context data, text records with row_num as additional context

    Returns:
        Iterator of tuples containing the row number and the processed text.
    """
    # configurable batch size using GLOBAL
    # configurable processes using GLOBAL
    logger.log("[cyan]Calling NLP pipeline...")
    doc_tuples = nlp.pipe(
        texts=text_tuples,
        batch_size=BATCH_SIZE,
        n_process=N_PROCESSES,
        as_tuples=True,
        disable=["tagger", "lemmatizer", "textcat"],
    )
    docs: list[Doc] = []
    for doc, context in doc_tuples:
        doc._.row_id = context["row_id"]
        docs.append(doc)
    return docs


def save_output(data: list[dict[str, str | bool | float]], batch: int):
    """Compresses and saves the output"""
    logger.log(f"[yellow]Compressing batch: {batch}...")
    out_path = (
        Path().cwd().parent / "data" / "ner_output" / "batches" / f"{batch}.feather"
    )
    logger.log(f"Sample of output data from batch {batch}")
    logger.log(data[0])
    pl.from_dicts(data).write_ipc(out_path, compression="lz4")
    logger.log(f"[green]Saved batch: {batch} with {len(data)} results.")


def process_results(docs: list[Doc], lookup: dict[str, Entity]) -> None:
    # use dynamically set global here for total to help with ETA
    output: list[dict[str, str | bool | float]] = []
    batch = 1
    for i, doc in tqdm(enumerate(docs), total=TOTAL, desc="Processing documents..."):
        if i % FILE_BATCH_SIZE == 0 and i != 0:
            batch += 1
            # compress and save the results
            save_output(data=output, batch=batch)
            # then clear output
            output.clear()
        # process the results
        for ent in doc.ents:
            for kb_ent in ent._.kb_ents:
                concept = lookup[kb_ent[0]]
                results: dict[str, str | bool | float] = {
                    "row_id": doc._.row_id,
                    "cui": concept.concept_id.strip(),
                    "name": concept.canonical_name.strip(),
                    "entity": ent.text.strip(),
                    "negated": ent._.negex,
                    "score": kb_ent[1],
                    "nlp_datetime": DT,
                }
                output.append(results)
    save_output(data=output, batch=batch)


def run(fpath: str, text_col: str, id_col: str) -> None:
    """Runs the script.

    Args:
        fpath (str): The path to the dataset.
        text_col (str): The text column to run Scispacy on.
        id_col (str): The identifier column
    """
    nlp, linker = setup()
    text_tuples = load_data(fpath=Path(fpath), text_col=text_col, id_col=id_col)

    # get the lookup table
    lookup = linker.kb.cui_to_entity

    docs_list = call_nlp(nlp=nlp, text_tuples=text_tuples)
    process_results(docs=docs_list, lookup=lookup)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="ner.py",
        description="Runs NER on the text in the provided dataset.",
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        required=True,
        help="The file to run NER on. Must be a feather file.",
    )
    parser.add_argument(
        "--text_col",
        "-t",
        type=str,
        required=True,
        help="The text column to run NER on.",
    )
    parser.add_argument(
        "--id_col",
        "-i",
        type=str,
        required=True,
        help="The identifier column.",
    )
    args = parser.parse_args()
    run(fpath=args.file, text_col=args.text_col, id_col=args.id_col)
    # run(
    #     fpath="../data/notes.feather",
    #     text_col="NOTE_TEXT",
    #     id_col="note_id",
    # )
