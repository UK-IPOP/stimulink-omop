from pathlib import Path
import polars as pl
import pandas as pd
from rich.console import Console

from paths import ID_SOURCE_DIR

pl.Config.set_fmt_str_lengths(80)

console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)


def select_aehr_note_text(row: dict[str, str]) -> str | None:
    """Select which field to use for the AEHR NOTE_TEXT column.

    Args:
        row (dict[str, str]): a row from the AEHR notes dataset

    Returns:
        str: the selected text
    """
    prefer1 = row["UnEditableChunkCompressed_PlainText"]
    prefer2 = row["EditableChunkCompressed_PlainText"]

    prefer1_valid = False
    prefer2_valid = False
    if prefer1 != "" and prefer1 is not None:
        # best case, most reliable
        prefer1_valid = True
    if (
        prefer2 != ""
        and prefer2 is not None
        and not prefer2.strip().startswith("<?xml")
    ):
        # second best case, less reliable, use only if not XML
        prefer2_valid = True

    if prefer1_valid and prefer2_valid:
        # if both valid, combine
        return prefer1 + "\n\n" + prefer2
    elif prefer1_valid:
        return prefer1
    elif prefer2_valid:
        return prefer2
    else:
        return None


aehr1 = pl.from_pandas(
    pd.read_csv(ID_SOURCE_DIR / "EX5765_COHORT1_AEHR_NOTES.csv", low_memory=False)
).lazy()
aehr2 = pl.from_pandas(
    pd.read_csv(ID_SOURCE_DIR / "EX5765_COHORT2_AEHR_NOTES.csv", low_memory=False)
).lazy()

aehr = pl.concat([aehr1, aehr2], how="vertical")

aehr = aehr.rename({"DOCUMENT_TYPE": "NOTE_TYPE", "RECORDED_DTM": "CREATED_DTM"})

aehr = aehr.with_columns(
    [
        pl.lit("AEHR").alias("NOTE_SOURCE"),
        pl.lit("UKHC").alias("DATA_SOURCE"),
        pl.lit("").alias("SPECIALTY"),
        pl.lit("0").alias("EPIC_LINES"),
        pl.col("VISIT_NUM").cast(str),
        (
            pl.col("EditableChunkCompressed_PlainText")
            .str.strip()
            .str.starts_with("<?xml")
            .alias("IS_XML")
        ),
        (
            pl.struct(
                [
                    "EditableChunkCompressed_PlainText",
                    "UnEditableChunkCompressed_PlainText",
                ]
            )
            .apply(select_aehr_note_text)
            .alias("NOTE_TEXT")
        ),
        (
            pl.when(
                pl.col("EditableChunkCompressed_PlainText")
                .str.strip()
                .str.starts_with("<?xml")
            )
            .then(pl.col("EditableChunkCompressed_PlainText"))
            .otherwise("")
            .alias("XML_DATA")
        ),
    ]
)

aehr = aehr.select(
    # apparently this keeps predicate push-down from breaking as opposed to `drop`
    pl.all().exclude(
        [
            "EditableChunkCompressed_PlainText",
            "UnEditableChunkCompressed_PlainText",
            "COHORT",
        ]
    )
)

# ### SCM Section

scm1 = pl.from_pandas(
    pd.read_csv(ID_SOURCE_DIR / "EX5765_COHORT1_SCM_NOTES.csv", low_memory=False)
).lazy()
scm2 = pl.from_pandas(
    pd.read_csv(ID_SOURCE_DIR / "EX5765_COHORT2_SCM_NOTES.csv", low_memory=False)
).lazy()

scm = pl.concat([scm1, scm2], how="vertical")

scm = scm.rename(
    {
        "DocumentName": "NOTE_TYPE",
        "CreatedWhen": "CREATED_DTM",
        "DetailText_PlainText": "NOTE_TEXT",
    }
)

scm = scm.with_columns(
    [
        pl.lit("SCM").alias("NOTE_SOURCE"),
        pl.lit("UKHC").alias("DATA_SOURCE"),
        pl.lit("").alias("SPECIALTY"),
        pl.lit(False).alias("IS_XML"),
        pl.lit("").alias("XML_DATA"),
        pl.lit("0").alias("EPIC_LINES"),
    ]
)

scm = scm.select(pl.all().exclude(["COHORT"]))

# ### EPIC Section

epic1 = pl.from_pandas(
    pd.read_csv(ID_SOURCE_DIR / "EX5765_COHORT1_EPIC_NOTES_LDS.csv")
).lazy()
epic2 = pl.from_pandas(
    pd.read_csv(ID_SOURCE_DIR / "EX5765_COHORT2_EPIC_NOTES_LDS.csv")
).lazy()

epic = pl.concat([epic1, epic2], how="vertical")

epic = epic.with_columns(
    [
        pl.lit("EPIC").alias("NOTE_SOURCE"),
        pl.lit("EPIC").alias("DATA_SOURCE"),
        pl.lit(False).alias("IS_XML"),
        pl.lit("").alias("XML_DATA"),
    ]
)

combined_epic_notes = (
    epic.unique()
    .sort("LINE")
    .groupby("NOTE_ID")
    .agg(
        [
            # note at this time in the query these are still elements and are not
            # colleted into a list until the end of this aggregation
            pl.col("LINE").max().cast(str).alias("EPIC_LINES"),
            pl.col("NOTE_TEXT"),
        ]
    )
    .with_columns(
        [
            pl.col("NOTE_TEXT").arr.join("\n\n"),
        ]
    )
)

epic = epic.select(pl.all().exclude(["COHORT", "LINE", "NOTE_TEXT"])).join(
    combined_epic_notes,
    on="NOTE_ID",
)

epic = epic.select(pl.all().exclude(["NOTE_ID"]))

# ### Combine Notes

# Now, before we actually combine these, we can run a sanity check that all of the columns match.

assert (
    sorted(aehr.columns) == sorted(scm.columns) == sorted(epic.columns)
), "Columns do not match"

# since each notes table have the same columns, we can write a little hack to `select` those columns in the same order
# since `concat` requires the same columns in the same order
sorted_cols = sorted(aehr.columns)
combined = (
    pl.concat(
        [
            # hacky hack :)
            aehr.select([pl.col(c) for c in sorted_cols]),
            scm.select([pl.col(c) for c in sorted_cols]),
            epic.select([pl.col(c) for c in sorted_cols]),
        ],
        how="vertical",
    )
    .with_columns(
        [
            pl.col("CREATED_DTM")
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f")
            .alias("CREATED_DTM"),
        ]
    )
    .with_row_count(name="note_id", offset=1)
)
print(combined.fetch().head(2))

# ? for some reason parquet broke on reading, but feather works fine
# dump notes to feather temp file for easier querying later.
# takes ~1-2 minutes and results in ~5 GB file
# uncompressed
collected = combined.collect()
collected.write_ipc(Path().cwd().parent / "data" / "notes.feather", compression="zstd")
