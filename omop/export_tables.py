import polars as pl
from rich.console import Console

from pathlib import Path

from combine_cohorts import combined as combined_cohorts
from combine_diagnoses import combined as combined_diagnoses
from combine_encounters import combined as combined_encounters
from combine_procedures import combined as combined_procedures
from combine_labs import combined as combined_labs
from combine_emars import combined as combined_emars
from combine_rx import combined as combined_rx

from combine_rx import combined as combined_rx
from combine_emars import combined as combined_emars

pl.Config.set_fmt_str_lengths(80)

console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)

DEST_DIR = Path().cwd().parent / "data" / "source_tables"

# notes first since manual
(
    pl.scan_ipc(Path().cwd().parent / "data" / "notes.feather")
    .collect()
    .to_pandas()
    .to_csv(DEST_DIR / "combined_notes.csv", index=False)
)
console.log("[green]Exported combined_notes[/green]")

# now loop over imports
for table, name in [
    (combined_cohorts, "combined_cohorts"),
    (combined_diagnoses, "combined_diagnoses"),
    (combined_emars, "combined_emars"),
    (combined_encounters, "combined_encounters"),
    (combined_labs, "combined_labs"),
    (combined_procedures, "combined_procedures"),
    (combined_rx, "combined_rx"),
]:
    table.with_row_count(offset=1).collect().to_pandas().to_csv(
        DEST_DIR / f"{name}.csv", index=False
    )
    console.log(f"[green]Exported {name}[/green]")

console.log("[green]Done.[/green]")
