from pathlib import Path

from ydata_profiling import ProfileReport
from rich.progress import track
import polars as pl

tables_dir = Path().cwd() / "data" / "omop_tables"
reports_dir = Path().cwd() / "data" / "omop_tables" / "profiles"
reports_dir.mkdir(parents=True, exist_ok=True)

files = list(tables_dir.glob("*.csv"))
for file in track(files, description="Profiling OMOP tables..."):
    if "Note" in file.stem or "Cohort" in file.stem:
        continue
    # ideally not read all as string, but this is a quick fix
    df = pl.read_csv(file, infer_schema_length=0).to_pandas()
    report = ProfileReport(
        df,
        title=file.stem,
        minimal=True,
        # explorative=True, # ?will take longer, valuable?
    )
    report.to_file(reports_dir / f"{file.stem}.html")
