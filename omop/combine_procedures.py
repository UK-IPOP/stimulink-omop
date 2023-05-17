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
    DEID_SOURCE_DIR / "EX5765_COHORT1_SCM_PROCEDURE_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
ukhc2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_SCM_PROCEDURE_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic1 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT1_EPIC_PROCEDURE_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_EPIC_PROCEDURE_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)


ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2])

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
        .arr[0]
        .str.strptime(pl.Date, "%Y-%m-%d", exact=True),
    ]
)

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
