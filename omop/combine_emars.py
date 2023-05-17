import polars as pl
from rich.console import Console

from paths import DEID_SOURCE_DIR

pl.Config.set_fmt_str_lengths(80)

console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)


ukhc1 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT1_SCM_EMAR_LDS.csv",
    infer_schema_length=0,
)
ukhc2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_SCM_EMAR_LDS.csv",
    infer_schema_length=0,
)
epic1 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT1_EPIC_EMAR_LDS.csv",
    infer_schema_length=0,
)
epic2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_EPIC_EMAR_LDS.csv",
    infer_schema_length=0,
)
ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2])

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
            [pl.col("DISCONTINUE_DATE"), pl.col("DISCONTINUE_TIME")], sep=" "
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
