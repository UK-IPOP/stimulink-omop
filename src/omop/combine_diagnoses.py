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
    UKHC_SOURCE1 / "EX3948_REV1_COHORT1_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
ukhc2 = pl.scan_csv(
    UKHC_SOURCE2 / "EX3948_REV1_COHORT2_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
epic1 = pl.scan_csv(
    EPIC_SOURCE / "EX4691_COHORT1_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
epic2 = pl.scan_csv(
    EPIC_SOURCE / "EX4691_COHORT2_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
epic3 = pl.scan_csv(
    EPIC_SOURCE2 / "EX5539_COHORT1_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)
epic4 = pl.scan_csv(
    EPIC_SOURCE2 / "EX5539_COHORT2_DIAGNOSIS_LDS.csv",
    infer_schema_length=0,
)

ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2, epic3, epic4])

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

# console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
