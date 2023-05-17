import polars as pl
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


ukhc1 = pl.scan_csv(
    ID_SOURCE_DIR / "EX5765_COHORT1_SCM_COHORT_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic1 = pl.scan_csv(
    ID_SOURCE_DIR / "EX5765_COHORT1_EPIC_COHORT_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
ukhc2 = pl.scan_csv(
    ID_SOURCE_DIR / "EX5765_COHORT2_SCM_COHORT_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)
epic2 = pl.scan_csv(
    ID_SOURCE_DIR / "EX5765_COHORT2_EPIC_COHORT_LDS.csv",
    infer_schema_length=0,
    try_parse_dates=True,
)

ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2])

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
