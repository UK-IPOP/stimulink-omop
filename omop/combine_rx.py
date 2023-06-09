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
    DEID_SOURCE_DIR / "EX5765_COHORT1_SCM_AEHR_RX_LDS.csv",
    infer_schema_length=0,
)
ukhc2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_SCM_AEHR_RX_LDS.csv",
    infer_schema_length=0,
)
epic1 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT1_EPIC_RX_LDS.csv",
    infer_schema_length=0,
)
epic2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_EPIC_RX_LDS.csv",
    infer_schema_length=0,
)


ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2]).drop("VISIT_NUM")  # unneeded and empty

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
