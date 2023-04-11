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

# here we can just use source1/source2 because the files have the cohort identifier in them as a column
# it is relevant to remember source1/source2  because the files within will have COHORT1/COHORT2 in their names
# so this mapping of 1/2 is easier than stimulant/opioid
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
    UKHC_SOURCE1 / "XXX",
    infer_schema_length=0,
)
ukhc2 = pl.scan_csv(
    UKHC_SOURCE2 / "XXX",
    infer_schema_length=0,
)
epic1 = pl.scan_csv(
    EPIC_SOURCE / "XXX",
    infer_schema_length=0,
)
epic2 = pl.scan_csv(
    EPIC_SOURCE / "XXX",
    infer_schema_length=0,
)
epic3 = pl.scan_csv(
    EPIC_SOURCE2 / "XXX",
    infer_schema_length=0,
)
epic4 = pl.scan_csv(
    EPIC_SOURCE2 / "XXX",
    infer_schema_length=0,
)

ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2, epic3, epic4])

# rename columns
ukhc = ukhc.rename({})

# rename columns
epic = epic.rename({})

# add columns
ukhc = ukhc.with_columns([])


# add columns
epic = epic.with_columns([])


# reorder columns
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))

# combine
combined = pl.concat([ukhc, epic])

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
