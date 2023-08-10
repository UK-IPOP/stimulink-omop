import argparse
import datetime
from itertools import zip_longest
import json
import multiprocessing
import pickle
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
from spacy.lang.en import English


c = Console()


c.log("Initializing models...")


nlp = spacy.load("en_core_med7_lg", enable=["ner", "sentencizer"])
nlp.add_pipe("sentencizer")

nlp.add_pipe("negex")

c.log("Done initializing models.")

c.log("Loading notes")
# read notes making sure no NA
notes = pl.scan_ipc("../data/notes.feather", memory_map=False).drop_nulls()
document_data = [
    (
        row[1],  # note
        {"row_num": row[0]},  # context
    )
    for row in tqdm(
        notes.select(["note_id", "NOTE_TEXT"]).fetch(500_000).to_numpy(),
        desc="Making tuples...",
    )
]
del notes
c.log("Done loading notes.")

sentence_data = []
for note, context in tqdm(document_data, desc="Sentencizing..."):
    for i, sent in enumerate(sent_tokenize(note)):
        context["sent_num"] = i
        sentence_data.append((sent, context))


del document_data

with open("med7_output.jsonl", "wb") as f:
    for doc, context in tqdm(
        nlp.pipe(sentence_data, as_tuples=True, batch_size=1_000, n_process=-1),
        desc="Processing notes with Med7...",
        total=len(sentence_data),
    ):
        if any(x in doc.text for x in {"meth", "cocaine", "heroin", "alcohol"}):
            print(doc.text)
        for i, ent in enumerate(doc.ents):
            print(i, "---", ent.text)
            data = {
                "row_num": context["row_num"],
                "sent_num": context["sent_num"],
                "entity": ent.text.strip(),
                "label": ent.label_,
                "negated": ent._.negex,
            }
            f.write(orjson.dumps(data) + b"\n")

c.log("Done NLP.")
