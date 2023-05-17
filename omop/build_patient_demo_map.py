from pathlib import Path
import polars as pl
from tqdm import tqdm
import json

# up here we are going to need a resolution algorithm for demographic fields
# this will be called for each person within the cohorts table on the encounters table
# and should return a dict mapping of the demographic fields
# this will be used to populate the person table


gender_cleaner = {
    "FEMALE": 8532,
    "MALE": 8507,
    "UNKNOWN": 0,
    "OTHER": 0,
}


race_cleaner: dict[str, int] = {
    "WHITE": 8527,
    "BLACK/AFR AMERI": 8516,
    "BLACK OR AFRICAN AMERICAN": 8516,
    "UNREPORT": 0,
    "UNKNOWN": 0,
    "ASIAN": 8515,
    "OTHER": 0,
    "AMERICAN INDIAN OR ALASKA NATIVE": 8657,
    "MULTI-RACIAL": 0,
    "AM INDIAN/ALASK": 8657,
    "OTHER ASIAN AMERICAN": 0,
    "SPANISH AMERICAN": 0,
    "CHINESE AMERICAN": 0,
    "CHINESE": 0,
    "JAPANESE AMERICAN": 0,
    "HAWAIIAN/PACISL": 8557,
    "DECLINE TO ANSWER": 0,
    "REFUSE": 0,
    "UNREPORTED": 0,
    "NATIVE HAWAIIAN": 8557,
    "MIDDLE EASTERN": 38003615,
    "ASIAN INDIAN": 38003574,
    "VIETNAMESE": 38003592,
    "FILIPINO": 38003581,
}

ethnicity_cleaner = {
    # 38003563  # hispanic
    # 38003564 # not
    "DECLINE TO ANSWER": 0,
    "REPORTED/REFUSED": 0,
    "UNKNOWN": 0,
    "HISPANIC/LATINO": 38003563,
    "HISPANIC, LATINO/A, OR SPANISH ORIGIN": 38003563,
    "NON HISPANIC/LATINO": 38003564,
    "NOT HISPANIC, LATINO/A, OR SPANISH ORIGIN": 38003564,
}


def validate_demographic_options():
    from combine_encounters import combined as encounters

    targets: list[tuple[str, dict[str, int]]] = [
        ("GENDER", gender_cleaner),
        ("RACE", race_cleaner),
        ("ETHNICITY", ethnicity_cleaner),
    ]
    col_names = [x[0] for x in targets]
    df = encounters.select(col_names).drop_nulls().collect()
    for col, cleaner in targets:
        unique_vals = df[col].unique().to_numpy().flatten()
        assert all(
            [val in cleaner for val in unique_vals]
        ), f"missing values in {col} for {[val for val in unique_vals if val not in cleaner.keys()]}"

    print("all demographic values are present in the cleaner dicts")


def resolve_demographics(edf: pl.LazyFrame) -> dict[str, tuple[str | None, int | None]]:
    """resolve for demographic fields gender, race, ethnicity.

    Args:
        edf (pl.LazyFrame): PRE-FILTERED encounters table for this person

    Returns:
        dict[str, tuple[str | None, int | None]]: _description_
    """
    targets: list[tuple[str, dict[str, int]]] = [
        ("GENDER", gender_cleaner),
        ("RACE", race_cleaner),
        ("ETHNICITY", ethnicity_cleaner),
    ]

    demo: dict[str, tuple[str | None, int | None]] = {}
    for col_name, cleaner in targets:
        # this is a np array of dicts
        # the dict is basically {col_name: value, counts: count_value}
        stats = (
            edf.select(pl.col(col_name).drop_nulls().value_counts(sort=True))
            .collect()
            .to_numpy()
            .flatten()
        )
        if len(stats) == 0:
            demo[col_name] = (None, None)
            continue
        max_count = stats[0]["counts"]
        common_rows = [
            row for row in stats if row["counts"] == max_count
        ]  # compare to max count
        if len(common_rows) != 1:
            # resolve, simply sort by the ID and take the lowest :/
            demo_val = sorted(common_rows, key=lambda x: x[col_name])[0][col_name]
            demo_id = cleaner[demo_val]
            demo[col_name] = (demo_val, demo_id)
        else:
            # one clear choice
            demo_val = common_rows[0][col_name]
            demo_id = cleaner[demo_val]
            demo[col_name] = (demo_val, demo_id)
    return demo


def find_all_demographics(
    patient_df: pl.LazyFrame,
) -> dict[str, dict[str, tuple[str | None, int | None]]]:
    from combine_encounters import combined as encounters

    # unique patient nums
    patient_nums = (
        patient_df.select(pl.col("PATIENT_NUM").unique()).collect().to_numpy().flatten()
    )
    patient_map: dict[str, dict[str, tuple[str | None, int | None]]] = {}
    for pt_num in tqdm(patient_nums):
        patient_map[pt_num] = resolve_demographics(
            encounters.filter(pl.col("PATIENT_NUM") == pt_num)
        )
    return patient_map


if __name__ == "__main__":
    from combine_cohorts import combined as patients

    validate_demographic_options()
    patient_map = find_all_demographics(patients)
    with open(Path().cwd().parent / "data" / "patient_demographics.json", "w") as f:
        json.dump(patient_map, f, indent=4, sort_keys=True)
