import pandas as pd
import polars as pl
from rich.console import Console

from pathlib import Path

pl.Config.set_fmt_str_lengths(80)

console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)

SOURCE_DIR = (
    Path().home()
    / "068IPOP_STIMuLINK-DataAnalytics"
    / "UKHC_5765-Harris"
    / "NON_SUD_FATAL_OD"
    / "UKHC_6065-Harris"
)

DEST_DIR = (
    Path().home()
    / "068IPOP_STIMuLINK-DataAnalytics"
    / "Source-Tables"
    / "NON_SUD_FATAL_OD"
)
DEST_DIR.mkdir(exist_ok=True)


ALL_TABLES: dict[str, pl.LazyFrame] = {}


################################
# COHORTS


ukhc = pl.scan_csv(
    SOURCE_DIR / "EX6065_SCM_COHORT_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic = pl.scan_csv(
    SOURCE_DIR / "EX6065_EPIC_COHORT_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)


ukhc = ukhc.rename({"BIRTH_DT": "BIRTH_DATE"})
epic = epic.rename({"RACE1": "RACE"})

ukhc = ukhc.with_columns(
    [
        pl.lit("UKHC").alias("DATA_SOURCE"),
    ]
)
epic = epic.with_columns(
    [
        pl.lit("").alias("ZIP_CD_4"),
        pl.lit("EPIC").alias("DATA_SOURCE"),
    ]
)
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))


# combine
combined = pl.concat([ukhc, epic]).with_columns(
    [
        pl.col("BIRTH_DATE").str.strptime(pl.Date, "%Y-%m-%d").alias("BIRTH_DATE"),
    ]
)

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")

cohorts = combined
ALL_TABLES["cohorts"] = cohorts

################################
# DIAGNOSES


ukhc = pl.scan_csv(
    SOURCE_DIR / "EX6065_SCM_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
epic = pl.scan_csv(
    SOURCE_DIR / "EX6065_EPIC_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)

ukhc = ukhc.rename({})

epic = epic.rename({})


ukhc = ukhc.with_columns(
    [
        pl.lit("UKHC").alias("DATA_SOURCE"),
    ]
)


epic = epic.with_columns(
    [
        pl.lit("EPIC").alias("DATA_SOURCE"),
    ]
)


# reorder columns
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))

# combine
combined = pl.concat([ukhc, epic])

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
diagnoses = combined
ALL_TABLES["diagnoses"] = diagnoses


################################
# EMARS


ukhc = pl.scan_csv(
    SOURCE_DIR / "EX6065_SCM_EMAR_LDS.csv",
    infer_schema_length=0,
)
epic = pl.scan_csv(
    SOURCE_DIR / "EX6065_EPIC_EMAR_LDS.csv",
    infer_schema_length=0,
)

# rename columns
ukhc = ukhc.rename(
    {
        "EMAR_GUID": "ORDER_MED_ID",
        "ORDER_NAME": "MED_ORDER_NAME",
        "ORDER_SET_NAME": "MED_NAME",
        "START_DTM": "MED_ADMINISTERED_DTTM",
        "TASK_STATUS_CODE": "MED_ADMIN_ACTION",
        "TASK_DOSE": "DOSE",
        "TASK_UOM": "DOSEUNIT",
        "FREQ_SUMMARY_LINE": "FREQUENCY",
        "TASK_ROUTE_CODE": "ROUTE",
        "FREQ_SUMMARY_LINE": "FREQUENCY",
    }
)

# rename columns
epic = epic.rename({})

# add columns
ukhc = ukhc.with_columns(
    [
        pl.col("STOP_DTM").alias("STOP_DATETIME"),
        pl.lit("").alias("GENERIC_NAME"),
        pl.lit("").alias("MED_SCHEDULED_DTTM"),
        pl.lit("").alias("PRIMARY_NDC"),
        pl.lit("").alias("SECOND_NDC"),
        pl.lit("").alias("THIRD_NDC"),
        pl.lit("").alias("FOURTH_NDC"),
        pl.lit("").alias("FIFTH_NDC"),
        pl.lit("").alias("DISCONTINUE_RSN"),
        pl.lit("UKHC").alias("DATA_SOURCE"),
    ]
).drop("STOP_DTM")


# add columns
epic = epic.with_columns(
    [
        pl.concat_str(
            [pl.col("DISCONTINUE_DATE"), pl.col("DISCONTINUE_TIME")], separator=" "
        ).alias("STOP_DATETIME"),
        pl.lit("").alias("MULTUM_CODE"),
        pl.lit("").alias("ORDER_TYPE"),
        pl.lit("").alias("TASK_REVIEW_CATGRY_CODE"),
        pl.lit("").alias("SOURCE_CODE"),
        pl.lit("").alias("ORDER_PRIORITY_CODE"),
        pl.lit("").alias("ORDERING_SERVICE"),
        pl.lit("").alias("ENTER_ROLE"),
        pl.lit("").alias("MODIFIER"),
        pl.lit("").alias("PRX_INDICATION_AI"),
        pl.lit("").alias("PRNREASONTEXT"),
        pl.lit("").alias("SUMMARY_LINE"),
        pl.lit("").alias("SPECIALINSTRUCTIONS"),
        pl.lit("").alias("TASK_SUMMARY_LINE"),
        pl.lit("").alias("TASK_COMMENT"),
        pl.lit("").alias("ORDER_ENTERED"),
        pl.lit("").alias("ORDER_ROUTE_CODE"),
        pl.lit("").alias("DOSAGE_LOW"),
        pl.lit("").alias("DOSAGE_HIGH"),
        pl.lit("").alias("UOM"),
        pl.lit("").alias("RATE_AMOUNT"),
        pl.lit("").alias("RATE_UOM"),
        pl.lit("").alias("ORDERED_AS_DISPLAY"),
        pl.lit("").alias("FORM_CODE"),
        pl.lit("").alias("SMM_DISPENSE_INFO"),
        pl.lit("").alias("THERAPEUTIC_CATEGORY"),
        pl.lit("").alias("ORDR_GUID"),
        pl.lit("EPIC").alias("DATA_SOURCE"),
    ]
).drop(["DISCONTINUE_DATE", "DISCONTINUE_TIME"])


# reorder columns
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))

# combine
combined = (
    pl.concat([ukhc, epic], how="vertical")
    .with_columns(
        [
            pl.when(pl.col("STOP_DATETIME").str.contains("."))
            .then(pl.col("STOP_DATETIME"))
            .otherwise(pl.col("STOP_DATETIME") + ".000")
            .keep_name()
        ]
    )
    .with_columns(
        [
            # now make datetimes
            pl.col("MED_ADMINISTERED_DTTM").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S.%3f", strict=False
            ),
            pl.col("STOP_DATETIME").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S.%3f", strict=False
            ),
        ]
    )
)

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
emars = combined
ALL_TABLES["emars"] = emars


################################
# ENCOUNTERS


ukhc = pl.scan_csv(
    SOURCE_DIR / "EX6065_SCM_ENCOUNTER_LDS.csv",
    infer_schema_length=0,
)
epic = pl.scan_csv(
    SOURCE_DIR / "EX6065_EPIC_ENCOUNTER_LDS.csv",
    infer_schema_length=0,
)

ukhc = ukhc.rename(
    {
        "AEHR_SMOKING_STATUS": "SMOKING_STATUS",
    }
)

epic = epic.rename(
    {
        "SEX_ASSIGNED_AT_BIRTH": "BIRTH_SEX",
        "CALC_WT_KG": "WT_KG",
        "RACE1": "RACE",
        "SEXUAL_ORIENTATION_LIST": "SEXUAL_ORIENTATION",
    }
)


ukhc = ukhc.with_columns(
    [
        pl.lit("UKHC").alias("DATA_SOURCE"),
        pl.lit("").alias("READMT_30DAY"),
        pl.lit("").alias("READMT_60DAY"),
        pl.lit("").alias("READMT_90DAY"),
        pl.lit("").alias("FINCL_CLASS_1"),
        pl.lit("").alias("DISCHRG_DISP"),
        pl.col("HT_CM").cast(pl.Float64),
        pl.lit("").alias("ILLICIT_DRUG_USE_PAST_YR"),
    ]
)


epic = epic.with_columns(
    [
        pl.lit("EPIC").alias("DATA_SOURCE"),
        (pl.col("CALC_HT_M").cast(pl.Float64) * 100.0).alias("HT_CM"),
        pl.lit("").alias("CENSUS_TRACT"),
        pl.lit("").alias("EDU_LEVEL"),
        # pl.lit("").alias("INS_TYPE"),
        pl.lit("").alias("PRONOUNS"),
        pl.lit("").alias("INS_TYPE"),
        pl.lit("").alias("ADMT_SRVC_CD_DES"),
        pl.lit("").alias("DISCHRG_DISP_CD_DES"),
        pl.lit("").alias("TOBACCO_USE_30DAY"),
        pl.lit("").alias("ILLICIT_DRUG_USE_PAST_YR"),
    ]
)


ukhc = ukhc.drop([])
# tobacco_user and smoking_status is all null
epic = epic.drop(["CALC_HT_M", "TOBACCO_USER"])

# reorder columns
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))

# combine
combined = pl.concat([ukhc, epic], how="vertical").with_columns(
    [
        pl.col("ADMT_DT").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"),
        pl.col("DISCHRG_DT").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"),
    ]
)

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
encounters = combined
ALL_TABLES["encounters"] = encounters


##############################
# LABS

ukhc = pl.scan_csv(
    SOURCE_DIR / "EX6065_SCM_LABS_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic = pl.scan_csv(
    SOURCE_DIR / "EX6065_EPIC_LABS_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)

ukhc = ukhc.rename(
    {
        "ORDR_NM": "ORDER_NAME",
        "LOINC_DISPLAYNAME": "LOINC_NAME",
        "ITEM_NAME": "LAB_NAME",
        "DESCRIPTION": "COMMON_NAME",
        "VAL_NUM": "VALUE_NUM",
        "UNIT_OF_MEASURE": "UNIT_OF_MEAS",
        "ABNORMALITY_CODE": "FLAG",
        "VAL_TXT": "VALUE_TXT",
        "TEXT_RESULT": "ORD_SUMMARY",
    }
)

epic = epic.rename(
    {
        "SPECIMENTYPE": "SPECIMEN_TYPE",
        "ORDR_NAME": "ORDER_NAME",
    }
)

ukhc = ukhc.with_columns(
    [
        pl.lit("").alias("TYPEOFCASE"),
        pl.lit("").alias("CASENAME"),
        pl.lit("").alias("SPECIMENNAME"),
        pl.lit("").alias("METHOD"),
        pl.lit("").alias("CASESTATUS"),
        pl.lit("").alias("SECTION"),
        pl.lit("").alias("LABORATORY"),
        pl.lit("").alias("SPECIMENSOURCE"),
        pl.lit("").alias("COLLECTIONCONTAINERTYPE"),
        pl.lit("").alias("SPECIMENPROTOCOL"),
        pl.lit("").alias("TESTNAME"),
        pl.lit("").alias("PATHOLOGYTYPE"),
        pl.lit("").alias("SOURCETYPE"),
        pl.lit("").alias("VERIFY_STATUS"),
        pl.lit("").alias("FLAG_ABBR"),
        pl.lit("UKHC").alias("DATA_SOURCE"),
    ]
)

epic = epic.with_columns(
    [
        pl.lit("").alias("REFERENCE_LOWER_LIMIT"),
        pl.lit("").alias("REFERENCE_UPPER_LIMIT"),
        pl.lit("").alias("CLUSTER_ID"),
        pl.lit("").alias("CODING_STD"),
        pl.lit("EPIC").alias("DATA_SOURCE"),
    ]
)

# reorder columns
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))

# combine
combined = (
    pl.concat([ukhc, epic], how="vertical")
    .with_columns(
        [
            pl.col("ENTERED").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S%.3f", exact=True
            ),
            pl.col("ORDR_REQESTD_DT_TM").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S%.3f", exact=True
            ),
            pl.col("ORDR_PERFRMD_DT_TM").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S%.3f", exact=True
            ),
            pl.col("VALUE_NUM").cast(pl.Float64),
            pl.col("REFERENCE_LOWER_LIMIT").str.replace(r">|NEG|<", "").str.strip(),
            pl.col("REFERENCE_UPPER_LIMIT").str.replace(r">|NEG|<", "").str.strip(),
        ]
    )
    .with_columns(
        [
            pl.when(
                pl.col(["REFERENCE_LOWER_LIMIT", "REFERENCE_UPPER_LIMIT"]).str.lengths()
                == 0
            )
            .then(None)
            .otherwise(pl.col(["REFERENCE_LOWER_LIMIT", "REFERENCE_UPPER_LIMIT"]))
            .keep_name()
        ]
    )
)


console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")

labs = combined
ALL_TABLES["labs"] = labs


##############################
# NOTES


# ### AEHR section


def select_aehr_note_text(row: dict[str, str]) -> str:
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
        return ""


aehr = pl.from_pandas(
    pd.read_csv(SOURCE_DIR / "EX6065_AEHR_NOTES.csv", low_memory=False)
).lazy()

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
            .apply(select_aehr_note_text, skip_nulls=False)
            .alias("NOTE_TEXT")
            .cast(str)
        ),
        (
            pl.when(
                pl.col("EditableChunkCompressed_PlainText")
                .str.strip()
                .str.starts_with("<?xml")
            )
            .then(pl.col("EditableChunkCompressed_PlainText"))
            .otherwise(pl.lit(""))
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

scm = pl.from_pandas(
    pd.read_csv(SOURCE_DIR / "EX6065_SCM_NOTES.csv", low_memory=False)
).lazy()

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

epic = pl.from_pandas(pd.read_csv(SOURCE_DIR / "EX6065_EPIC_NOTES_LDS.csv")).lazy()

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
            pl.col("NOTE_TEXT").list.join("\n\n"),
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

notes = combined
ALL_TABLES["notes"] = notes

##############################################
# PROCEDURES


ukhc = pl.scan_csv(
    SOURCE_DIR / "EX6065_SCM_PROCEDURE_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic = pl.scan_csv(
    SOURCE_DIR / "EX6065_EPIC_PROCEDURE_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)

ukhc = ukhc.rename(
    {
        "SERVICE_DT": "SERVICE_DATE",
        "CHRG_PROCDR_CD": "CPT_CODE",
        "CHRG_PROCDR_CD_DES": "CPT_DESCR",
        "CHRG_MODFR_VAL": "CPT_MODIFIERS",
        "UNITS_OF_SVC": "CPT_QUANTITY",
    }
)

epic = epic.rename({})


ukhc = ukhc.with_columns(
    [
        pl.lit("UKHC").alias("DATA_SOURCE"),
    ]
)


epic = epic.with_columns(
    [
        pl.lit("").alias("SRC"),
        pl.lit("EPIC").alias("DATA_SOURCE"),
    ]
)


# reorder columns
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))

# combine
combined = pl.concat([ukhc, epic]).with_columns(
    [
        # split on string since some dates have "00:00:00.000" as extra data
        pl.col("SERVICE_DATE")
        .str.split(" ")
        .list.first()
        .str.strptime(pl.Date, "%Y-%m-%d", exact=True),
    ]
)

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
procedures = combined
ALL_TABLES["procedures"] = procedures

##############################################
# RX

ukhc = pl.scan_csv(
    SOURCE_DIR / "EX6065_SCM_AEHR_RX_LDS.csv",
    infer_schema_length=0,
)

epic = pl.scan_csv(
    SOURCE_DIR / "EX6065_EPIC_RX_LDS.csv",
    infer_schema_length=0,
)


# rename columns
ukhc = ukhc.rename(
    {
        "DISPLAY_NAME": "DESCRIPTION",
        "DRUG_NAME": "ORDER_SET_NAME",
        "FILL_DT": "ORDR_SCHEDULED_TIME",
        "UNIT_OF_MEAS": "DOSE_UOM",
        "ROUTE_OF_ADMIN": "ROUTE",
        "LAST_FILL_END": "ORDER_STOP_DTTM",
        "INSTRUCTIONS": "ORDER_SIG",
        "TCGPI_ID": "GPI",
        "QTY_DISPENSE": "QUANTITY",
        "REFILL": "REFILLS",
    }
)

# rename columns
epic = epic.rename({})

# add columns
ukhc = ukhc.with_columns(
    [
        pl.lit("").alias("ORDER_MED_ID"),
        pl.lit("").alias("DISPLAY_NAME"),
        pl.lit("").alias("ORDR_SCHEDULED_TIME"),
        pl.lit("").alias("ORDER_START_DTTM"),
        pl.lit("").alias("ORDR_PERFORMED_DTTM"),
        pl.lit("").alias("FREQ_NAME"),
        pl.lit("").alias("RSN_FOR_DISCON_DESCR"),
        pl.lit("").alias("ORDER_PRIORITY_DESCR"),
        pl.lit("").alias("ORDER_ENTRD_DTTM"),
        pl.lit("").alias("EMAR_LINE"),
        pl.lit("").alias("HCPCS_CODE"),
        pl.lit("").alias("ORDER_SECTION_NAME"),
        pl.lit("").alias("ORDER_UPDT_DTTM"),
        pl.lit("").alias("WAS_TIMELY_ADMIN_DESCR"),
        pl.lit("").alias("INF_RATE_UOM"),
        pl.lit("").alias("INF_DURATION"),
        pl.lit("").alias("INF_DURATION_UOM"),
        pl.lit("").alias("MORPHINE_EQUIV_MG_DOSE"),
        pl.lit("").alias("MORPHINE_EQUIV_MG_PER_HR_RATE"),
        pl.lit("").alias("DISP_AS_WRITTEN_YN"),
        pl.lit("").alias("RSN_FOR_DISCON_C"),
        pl.lit("UKHC").alias("DATA_SOURCE"),
        # NEW FOR THIS script
        pl.lit("").alias("VISIT_NUM"),
    ]
)


# add columns
epic = epic.with_columns(
    [
        pl.lit("").alias("COMMENTS"),
        pl.lit("").alias("TCGPI_NAME"),
        pl.lit("").alias("ROOT_CLASSIFICATION"),
        pl.lit("").alias("SECONDARY_CLASSIFICATION"),
        pl.lit("").alias("DAYS_TO_TAKE"),
        pl.lit("").alias("DAYS_SUPPLY"),
        pl.lit("").alias("FIRST_FILL_END"),
        pl.lit("").alias("FORM"),
        pl.lit("").alias("STRENGTH"),
        pl.lit("EPIC").alias("DATA_SOURCE"),
    ]
)


# reorder columns
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))

# combine
combined = (
    pl.concat([ukhc, epic], how="vertical")
    .with_columns(
        [
            pl.when(pl.col("ORDER_STOP_DTTM").str.contains("."))
            .then(pl.col("ORDER_STOP_DTTM"))
            .otherwise(pl.col("ORDER_STOP_DTTM") + ".000")
            .keep_name()
        ]
    )
    .with_columns(
        [
            pl.col("ORDER_STOP_DTTM").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S.%3f", strict=False
            ),
            pl.col("ORDER_START_DTTM").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S.%3f", strict=False
            ),
        ]
    )
)

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")

rx = combined
ALL_TABLES["rx"] = rx

##############################################
# exporting section

for name, table in ALL_TABLES.items():
    name = name.upper()
    console.rule(f"{name}")
    data = table.collect()
    if name == "NOTES":
        data.to_pandas().to_csv(DEST_DIR / f"{name}.csv", index=False)
    else:
        data.write_csv(DEST_DIR / f"{name}.csv")
    console.log(f"Wrote {len(data)} rows to {name}.csv")
