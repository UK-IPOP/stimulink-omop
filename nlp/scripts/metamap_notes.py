import argparse
import subprocess
from pathlib import Path
from typing import Any

import polars as pl
from rich.console import Console
from p_tqdm import p_uimap
from tqdm import tqdm
from nltk.tokenize import sent_tokenize
import nltk

nltk.download("punkt")

c = Console()


def setup_paths():
    """Sets up the output paths for the script."""
    p = Path() / "data" / "metamap_output"
    p.mkdir(exist_ok=True, parents=True)


def load_data(fpath: str, index_col: str, target_col: str) -> list[dict[str, Any]]:
    """Loads the data from file provided.

    EXPECTS FEATHER FILE

    Returns:
        list[dict[str, Any]]: The table data
    """
    # manaully reset index for now
    data: list[dict[str, Any]] = (
        pl.read_ipc(fpath).drop_nulls(subset=[index_col, target_col]).to_dicts()
    )
    c.log(f"Table has {len(data)} rows")
    return data


def start_servers():
    subprocess.run(
        [
            "~/public_mm/bin/skrmedpostctl",
            "start",
        ]
    )
    subprocess.run(
        [
            "~/public_mm/bin/wsdserverctl",
            "start",
        ]
    )


def stop_servers():
    subprocess.run(
        [
            "~/public_mm/bin/skrmedpostctl",
            "stop",
        ]
    )
    subprocess.run(
        [
            "~/public_mm/bin/wsdserverctl",
            "stop",
        ]
    )


def command(x: str) -> list[str] | None:
    """Pass in text for metamap

    Args:
        x (str): Text to pass to metamap

    Returns:
        list[str]|None: The output of metamap
    """

    try:
        result = subprocess.run(
            ["./nlp/scripts/metamap.sh", x.encode("utf-8")], stdout=subprocess.PIPE
        )
        parsed_result = result.stdout.decode("utf-8")
        mm_result = [x.strip() for x in parsed_result.split("\n")][
            1:-1
        ]  # skip headers and final newline
        return mm_result

    except OSError as e:
        print(f"[red]FAILED: {e}")
        return None


def process_row(item: tuple[int, list[str]]) -> list[str]:
    results: list[str] = []
    # run metamap
    for sent in item[1]:
        output = command(sent)
        if output is None:
            continue
        for line in output:
            results.append(f"{item[0]}|{line.strip()}\n")
    return results


def run(fpath: str, text_col: str, id_col: str) -> None:
    """Runs the script.

    This script runs metamap on the notes in the dataset.

    It will write the results to `output.txt` in the data/metamap_output directory.

    The results will include all fields from the original dataset, plus the
    metamap results EXCEPT the original note text (to save space).

    Args:
        fpath (str): The path to the dataset.
        text_col (str): The text column to run metamap on.
        id_col (str): The identifier column
    """
    setup_paths()
    data = load_data(fpath=fpath, index_col=id_col, target_col=text_col)

    c.log(f"Running MetaMap on [cyan]{text_col}[/]...")
    c.log("=====================")

    c.log(f"Tracking [cyan]{id_col}[/]...")
    c.log("=====================")

    # c.log("[yellow]Starting MetaMap servers...")
    # start_servers()

    # this current approach still requires joining later on which I don't think is a huge problem
    # and is probably OMOP compliant
    c.log("[yellow]Prepping sentences and IDs...")
    id_list = [x[id_col] for x in data]
    text_list = [x[text_col] for x in data]
    sent_list = [sent_tokenize(x) for x in text_list]
    del data

    with open(Path("data") / "metamap_output" / "output.txt", "w") as f:
        # for result in p_uimap(process_row, list(zip(id_list, sent_list)), num_cpus=40):
        for id_, sent in tqdm(zip(id_list, sent_list)):
            for line in process_row((id_, sent)):
                # print("writing line:", line)
                f.write(line)

        # c.log("[yellow]Stopping MetaMap servers...")
        # stop_servers()
        c.log("[green]Done!")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="metamap_notes",
        description="Runs metamap on the text in the provided dataset.",
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        help="The file to run metamap on. Must be a parquet file.",
    )
    parser.add_argument(
        "--target",
        "-t",
        type=str,
        help="The column to run metamap on. Must be a string column.",
    )
    parser.add_argument(
        "--id",
        "-i",
        type=str,
        help="The identifier column to track. Must be a string column.",
    )
    args = parser.parse_args()
    run(
        fpath=args.file,
        text_col=args.target,
        id_col=args.id,
    )
    # run(
    #     fpath="sample.csv",
    #     text_col="note_text",
    #     id_col="row_num",
    # )
