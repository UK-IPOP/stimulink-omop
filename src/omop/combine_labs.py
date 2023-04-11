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

UKHC_SOURCE1 = (
    Path().home()
    / "068IPOP_STIMuLINK-Team"
    / "UKHC_3948_REV1-Harris"
    / "stimulant_population_deid"
)
UKHC_SOURCE2 = (
    Path().home()
    / "068IPOP_STIMuLINK-Team"
    / "UKHC_3948_REV1-Harris"
    / "opioid_population_deid"
)
EPIC_SOURCE = Path().home() / "068IPOP_STIMuLINK-Team" / "UKHC_4691-Harris"
EPIC_SOURCE2 = Path().home() / "068IPOP_STIMuLINK-Team" / "UKHC_5539-Harris"

ukhc1 = pl.scan_csv(
    UKHC_SOURCE1 / "EX3948_REV1_COHORT1_LABS_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
ukhc2 = pl.scan_csv(
    UKHC_SOURCE2 / "EX3948_REV1_COHORT2_LABS_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic1 = pl.scan_csv(
    EPIC_SOURCE / "EX4691_COHORT1_LABS_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic2 = pl.scan_csv(
    EPIC_SOURCE / "EX4691_COHORT2_LABS_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic3 = pl.scan_csv(
    EPIC_SOURCE2 / "EX5539_COHORT1_LABS_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic4 = pl.scan_csv(
    EPIC_SOURCE2 / "EX5539_COHORT2_LABS_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)

ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2, epic3, epic4])

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


# console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
