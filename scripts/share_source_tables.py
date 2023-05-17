from pathlib import Path
import shutil
from rich.console import Console
from rich.progress import track

console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)

source_dir = Path().cwd().parent / "data" / "source_tables"
dest_dir = Path().home() / "068IPOP_STIMuLINK-DataAnalytics" / "Source-Tables"
dest_dir.mkdir(parents=True, exist_ok=True)

files = list(source_dir.glob("*.csv"))
for file in track(files, description="Moving files"):
    dest_file = dest_dir / file.name
    shutil.move(file, dest_file)
    console.log(f"\n[green]Moved {file.name}[/green]")

# copy README
for file in source_dir.glob("README.html"):
    dest_file = dest_dir / file.name
    shutil.move(file, dest_file)
    console.log(f"\n[green]Moved {file.name}[/green]")
