import polars as pl
import polars as pl
from rich.console import Console
from typing import TypedDict


pl.Config.set_fmt_str_lengths(80)

console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)


class Row(TypedDict):
    stimulant: bool
    opioid: bool
    both: bool
    neither: bool


def resolve_cohort(row: Row) -> str:
    if row["both"] is True:
        return "both"
    elif row["neither"] is True:
        return "neither"
    elif row["opioid"] is True:
        return "opioid"
    elif row["stimulant"] is True:
        return "stimulant"
    else:
        raise ValueError(f"no cohort found, row: {row}")


stimulant_regex = r"^F14.|^F15.|^T40.5|^T43.6|^T43.62"
opioid_regex = r"F11|^T40.[012346]"


# here what we want to do is actually look at the diagnosis file
# and see which patients have which diagnoses
# if they have stimulant diagnoses, then they are in the stimulant cohort
# if they have opioid diagnoses, then they are in the opioid cohort
# if they have both, then they are in the both cohort
def run() -> list[dict[str, str]]:
    from omop.combine_diagnoses import combined as df

    cleaned = (
        df.select(
            [
                "PATIENT_NUM",
                "DIAGNOSIS",
                pl.col("DIAGNOSIS")
                .str.strip()
                .str.contains(stimulant_regex)
                .alias("stimulant"),
                pl.col("DIAGNOSIS")
                .str.strip()
                .str.contains(opioid_regex)
                .alias("opioid"),
            ]
        )
        .groupby("PATIENT_NUM")
        .agg(
            [
                pl.col("DIAGNOSIS"),
                pl.col("DIAGNOSIS").count().alias("num_diagnoses"),
                pl.col("stimulant").any(),
                pl.col("opioid").any(),
            ]
        )
        .with_columns(
            [
                pl.when(
                    pl.col("stimulant").eq(True) & pl.col("opioid").eq(True),
                )
                .then(True)
                .otherwise(False)
                .alias("both"),
                pl.when(
                    pl.col("stimulant").eq(False) & pl.col("opioid").eq(False),
                )
                .then(True)
                .otherwise(False)
                .alias("neither"),
            ]
        )
        .with_columns(
            [
                pl.struct(["stimulant", "opioid", "both", "neither"])
                .apply(resolve_cohort)
                .alias("cohort"),
            ]
        )
        .select(pl.struct(["PATIENT_NUM", "cohort"]))
    )
    rows = cleaned.collect().to_numpy().flatten().tolist()
    console.log("[green]Done.[/green]")
    return rows


if __name__ == "__main__":
    data = run()
