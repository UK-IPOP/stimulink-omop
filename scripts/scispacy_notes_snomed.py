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
from negspacy.negation import Negex


print("Downloading `punkt` sentence tokenizer...")
# nltk.download("punkt")

c = Console()

SNOMED_LINKER_PATHS = LinkerPaths(
    ann_index="models/nmslib_index.bin",
    tfidf_vectorizer="models/tfidf_vectorizer.joblib",
    tfidf_vectors="models/tfidf_vectors_sparse.npz",
    concept_aliases_list="models/concept_aliases.json",
)


class SnomedKnowledgeBase(KnowledgeBase):
    def __init__(
        self,
        file_path: str = "snomed.jsonl",
    ):
        super().__init__(file_path)


# # Admittedly this is a bit of a hack, because we are mutating a global object.
# # However, it's just a kind of registry, so maybe it's ok.
DEFAULT_PATHS["snomed"] = SNOMED_LINKER_PATHS
DEFAULT_KNOWLEDGE_BASES["snomed"] = SnomedKnowledgeBase


c.log("Initializing Scispacy...")

# only ner?
nlp = spacy.load("en_core_sci_lg", enable=["ner"])
nlp.add_pipe("sentencizer")

# nlp.add_pipe("abbreviation_detector")
linker = CandidateGenerator(name="snomed")
nlp.add_pipe("negex")
nlp.add_pipe(
    "scispacy_linker",
    config={
        "resolve_abbreviations": True,
        "max_entities_per_mention": 1,
        "k": 30,  # default is 30
        "threshold": 0.9,  # default is 0.7
        "filter_for_definitions": True,  # this is the big one! limits to only entities with definitions in knowledge base, oh was already true by default
        "linker_name": "snomed",  # ! switched to SNOMED
    },
)

c.log("Done initializing Scispacy.")

c.log("Loading notes")
# read notes making sure no NA
notes = pl.scan_ipc("../data/notes.feather").drop_nulls(subset=["NOTE_TEXT"])
document_data = [
    (
        row[1],  # note
        {"row_num": row[0]},  # context
    )
    for row in tqdm(
        notes.select(["note_id", "NOTE_TEXT"]).collect().to_numpy(),
        desc="Making tuples...",
    )
]
del notes
c.log("Done loading notes.")



c.log("NLP...")

with open("snomed_output.jsonl", "wb") as f:
    for doc, context in tqdm(
        nlp.pipe(document_data, as_tuples=True, batch_size=1_000, n_process=-1),
        desc="Processing notes...",
        total=len(document_data),
    ):
        for ent in doc.ents:
            for kb_ent in ent._.kb_ents:
                concept: LinkedEntity = linker.kb.cui_to_entity[kb_ent[0]]
                data = {
                    "row_num": context["row_num"],
                    # ! changed to `suid` since its not UMLS cui anymore
                    "suid": concept.concept_id.strip(),
                    "name": concept.canonical_name.strip(),
                    "score": kb_ent[1],
                    "negated": ent._.negex,
                    "entity": ent.text.strip(),
                }
                f.write(orjson.dumps(data) + b"\n")

c.log("Done NLP.")
