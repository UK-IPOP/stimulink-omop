from collections import defaultdict
import pandas as pd
from dataclasses import dataclass
from pathlib import Path
from rich import print
from rich.progress import track
from tqdm import tqdm
from typing import Optional
import orjson


@dataclass(slots=True, kw_only=True)
class Entity:
    concept_id: str
    canonical_name: str
    aliases: list[str]
    types: list[str]
    definition: Optional[str] = None


# concepts themselves
concepts: set[str] = set()
with open(
    "./data/knowledge_bases/sct2_Concept_Snapshot_US1000124_20220901.txt", "r"
) as f:
    for i, line in enumerate(f):
        if i == 0:
            continue
        parts = line.strip().split("\t")
        concept_id = parts[0].strip()
        active = parts[2].strip()
        if active == "1":
            concepts.add(concept_id)

print(f"Found {len(concepts):,} ACTIVE concepts")


print("loading descriptings 1...")
# custom loading because of weird file formatting
data: list[tuple[int, str, str]] = []
with open(
    "./data/knowledge_bases/sct2_Description_Snapshot-en_US1000124_20220901.txt", "r"
) as f:
    for i, line in enumerate(f):
        if i == 0:
            continue
        parts = line.strip().split("\t")
        concept_id = parts[4].strip()
        active_defintion = parts[2].strip()
        if active_defintion == "1" and concept_id in concepts:
            type_id = parts[6].strip()
            term_text = parts[7].strip()
            data.append((int(concept_id), type_id, term_text))

print("loading descriptings 2...")
with open(
    "./data/knowledge_bases/sct2_TextDefinition_Snapshot-en_US1000124_20220901.txt", "r"
) as f:
    for i, line in enumerate(f):
        if i == 0:
            continue
        parts = line.strip().split("\t")
        concept_id = parts[4].strip()
        active_defintion = parts[2].strip()
        if active_defintion == "1" and concept_id in concepts:
            type_id = parts[6].strip()
            term_text = parts[7].strip()
            data.append((int(concept_id), type_id, term_text))

print("loading making df......")
df = pd.DataFrame(
    data,
    columns=[
        "conceptId",
        "typeId",
        "term",
    ],
)

df["conceptId"] = df["conceptId"].astype(int)

print(df.head())

print(f"Descriptions has {len(df):,} rows")


description_types = {
    "900000000000003001": "FSN",
    "900000000000013009": "Synonym",
    "900000000000550004": "Definition",
}

print("loading mrsty...")
# load semantic types
semantic_type_lookup: dict[str, list[str]] = defaultdict(list)
with open("./data/knowledge_bases/MRSTY.RRF", "r") as f:
    for line in f:
        cui, tui = line.split("|")[:2]
        semantic_type_lookup[cui].append(tui)

print("loading mrconso...")
# load snomed to cui lookup
snomed_to_cui: dict[str, str] = {}
with open("./data/knowledge_bases/MRCONSO.RRF", "r") as f:
    for line in f:
        parts = line.split("|")[:-1]
        if parts[11] == "SNOMEDCT_US":
            snomed_to_cui[parts[9]] = parts[0]

# del data

excluded: set[int] = set()
print("creating knowledge")
concept_data: dict[int, Entity] = {}
for (concept_id, type_id, term_text) in track(data):

    umls_cui = snomed_to_cui.get(str(concept_id))
    if umls_cui is None:
        excluded.add(concept_id)
        continue
        # raise ValueError(f"Could not find UMLS CUI for {concept_id}")
    if concept_id not in concept_data:
        concept_data[concept_id] = Entity(
            concept_id=str(concept_id),
            canonical_name="",
            aliases=[],
            types=[],
        )
    # manipulate inplace
    if type_id == "900000000000003001":
        concept_data[concept_id].canonical_name = term_text
    elif type_id == "900000000000013009":
        concept_data[concept_id].aliases.append(term_text)
    elif type_id == "900000000000550004":
        concept_data[concept_id].definition = term_text
    else:
        raise ValueError(f"Unknown type id {type_id}")

    # once record initialized, add semantic types
    concept_data[concept_id].types = semantic_type_lookup[umls_cui]


print("writing to file...")
with open("./data/knowledge_bases/snomed.jsonl", "wb") as f:
    count = 0
    for entity in track(concept_data.values()):
        f.write(orjson.dumps(entity) + b"\n")
        count += 1


print(f"Saved {count:,} entities to snomed.jsonl")
print(f"Invalid snomed ids: {len(excluded)}")

# write excluded ids
with open("./data/knowledge_bases/excluded_snomed_ids.txt", "w") as f:
    for snomed_id in excluded:
        f.write(str(snomed_id) + "\n")
