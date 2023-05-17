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
    DEID_SOURCE_DIR / "EX5765_COHORT1_SCM_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
ukhc2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_SCM_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
epic1 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT1_EPIC_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
epic2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_EPIC_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2])

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
