from pathlib import Path
import shutil
from rich.progress import track
from rich.console import Console

console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)

source_dir = Path().cwd() / "data" / "omop_tables"
dest_dir1 = Path().home() / "068IPOP_STIMuLINK-DataAnalytics" / "OMOP"
dest_dir2 = Path().home() / "068IPOP_STIMuLINK-Team" / "OMOP"
dest_dir1.mkdir(parents=True, exist_ok=True)
dest_dir2.mkdir(parents=True, exist_ok=True)

# use shutil to move to external drive,
# use copy to copy to same-drive
# and retain original for later moving

files = list(source_dir.glob("*.csv"))
for file in track(files, description="Copying files"):
    if file.name == "Location.csv" or file.name == "Note.csv":
        # only put in one folder
        dest_file1 = dest_dir1 / file.name
        shutil.move(file, dest_file1)
        console.log(f"Moved {file.name} to {dest_dir1}.")
    else:
        # put in both folders
        dest_file1 = dest_dir1 / file.name
        dest_file2 = dest_dir2 / file.name
        shutil.copy(file, dest_file1)
        console.log(f"Copied {file.name} to {dest_dir1}.")
        shutil.move(file, dest_file2)
        console.log(f"Moved {file.name} to {dest_dir2}.")


# copy README
readme = source_dir / "README.html"
readme1 = dest_dir1 / "README.html"
readme2 = dest_dir2 / "README.html"
shutil.copy(readme, readme1)
console.log(f"Copied {readme.name} to {dest_dir1}.")
shutil.move(readme, readme2)
console.log(f"Moves {readme.name} to {dest_dir2}.")
