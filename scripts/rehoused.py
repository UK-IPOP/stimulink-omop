from rehoused_nlp import calculate_rehoused
import pandas as pd
from tqdm import tqdm
from rehoused_nlp import build_nlp

import warnings

warnings.filterwarnings("ignore")

nlp = build_nlp()

df = (
    pd.read_feather("../data/notes.feather")
    .drop(
        columns=[
            "DATA_SOURCE",
            "EPIC_LINES",
            "IS_XML",
            "NOTE_SOURCE",
            "NOTE_TYPE",
            "SPECIALTY",
            "XML_DATA",
        ]
    )
    .dropna(subset="NOTE_TEXT")
    # .sample(100_000, random_state=42)
)
print("Input:")
print(df.head())


docs = list(
    nlp.pipe(
        tqdm(df["NOTE_TEXT"], desc="NLP"),
        # batch_size=1_000_000,
        batch_size=1_000,
        # n_process=4,
    )
)

df["docs"] = docs
df["document_classification"] = [
    doc._.document_classification for doc in tqdm(docs, desc="loop")
]

df["days_since_index"] = (
    df.sort_values(by="CREATED_DTM")
    .groupby("PATIENT_NUM")["CREATED_DTM"]
    .diff()
    .dt.days
)

with open("notes_with_docs.pkl", "wb") as f:
    import pickle

    pickle.dump(df, f)


rehoused: pd.DataFrame = calculate_rehoused(
    df=df,
    window_size=30,
    patient_col="PATIENT_NUM",  # pseudo-patient-id
    doc_class_col="document_classification",
    time_col="days_since_index",
)
print("Output:")
print(rehoused.shape)
rehoused = rehoused.sort_values(by=["PATIENT_NUM", "time_window"])
print(rehoused)
rehoused.to_parquet("rehoused.parquet")
