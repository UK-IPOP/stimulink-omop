import itertools
import json
import pandas as pd
import polars as pl
from rich.console import Console

from pathlib import Path

from tqdm import tqdm


pl.Config.set_fmt_str_lengths(80)

# TODO: confirm `unique_pts`` function works correctly?
# there is this weird behavior where it functions correctly but then the
# output persons table has the correct IDs but has less rows than expected...


console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)

KNOWLEDGE_DIR = Path().cwd().parent / "data" / "knowledge_bases"
DEST_DIR = Path().cwd().parent / "data" / "omop_tables"


def unique_pts(df: pl.LazyFrame) -> pl.LazyFrame:
    # this is NOT perfect
    # we basically look for dupe patient_nums and then if there is a dupe
    # we rely on EPIC data as being most recent
    dff = df.drop(["COHORT", "VISIT_NUM"]).unique()
    ids = set()
    dupe_set = set()
    for row in dff.collect().to_dicts():
        id_ = row["PATIENT_NUM"]
        if id_ in ids:
            dupe_set.add(id_)
        else:
            ids.add(id_)

    dupes = (
        dff.filter(pl.col("PATIENT_NUM").is_in(list(dupe_set)))
        .filter(pl.col("DATA_SOURCE").eq("EPIC"))
        .sort("PATIENT_NUM")
        .groupby("PATIENT_NUM", maintain_order=True)
        # just take first row
        .agg(pl.all().take(0))
    )
    assert dupes.collect().shape[0] == len(dupe_set)
    original = dff.filter(~pl.col("PATIENT_NUM").is_in(list(dupe_set)))
    assert original.collect().shape[0] == len(ids) - len(dupe_set)
    original = original.select(sorted(original.columns))
    dupes = dupes.select(sorted(dupes.columns))
    combined = pl.concat([original, dupes], how="vertical")
    total_pts = combined.collect().shape[0]
    assert total_pts == len(ids)
    return combined


def make_patient_id_lookup() -> dict[int, str]:
    from combine_cohorts import combined as df

    return {
        row[1]: row[0]
        for row in (
            unique_pts(df)
            .with_row_count(name="person_id", offset=1)
            .select(["person_id", "PATIENT_NUM"])
            .collect()
            .to_numpy()
        )
    }


patient_num_to_id = make_patient_id_lookup()
assert (
    len(patient_num_to_id) == 26918
), f"Should be 26,918 patients, got {len(patient_num_to_id)}"
print(f"patient_num_to_id: {len(patient_num_to_id)}")


def load_patient_map() -> dict[str, dict[str, tuple[str, int]]]:
    with open(Path().cwd().parent / "data" / "patient_demographics.json", "r") as f:
        return json.load(f)


def make_location_lookup() -> dict[str, int]:
    # same steps as in `omop_locations` so the ids should be the same
    from combine_cohorts import combined

    df = unique_pts(combined)
    return {
        row[1]: row[0]
        for row in df.select("PATIENT_NUM")
        .with_row_count(name="location_id", offset=1)
        .select(["location_id", "PATIENT_NUM"])
        .collect()
        .to_numpy()
    }


location_lookup = make_location_lookup()


def omop_persons():
    from combine_cohorts import combined

    df = unique_pts(combined)
    # here we want to just read the patient map from file
    patient_map = load_patient_map()

    def lookup(pid: str, field: str, pos: int) -> str | int:
        return patient_map[pid][field][pos]

    old_cols = df.columns

    omop = df.with_columns(
        [
            #
            # required
            pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
            pl.col("BIRTH_DATE").dt.year().alias("year_of_birth"),
            pl.col("PATIENT_NUM")
            .apply(lambda x: lookup(x, "GENDER", 1))  # type: ignore
            .alias("gender_concept_id"),
            pl.col("PATIENT_NUM")
            .apply(lambda x: lookup(x, "RACE", 1))  # type: ignore
            .alias("race_concept_id"),
            pl.col("PATIENT_NUM")
            .apply(lambda x: lookup(x, "ETHNICITY", 1))  # type: ignore
            .alias("ethnicity_concept_id"),
            #
            # optional
            pl.col("BIRTH_DATE").dt.month().alias("month_of_birth"),
            pl.col("BIRTH_DATE").dt.day().alias("day_of_birth"),
            pl.col("PATIENT_NUM").map_dict(location_lookup).alias("location_id"),
            # for linkage back to source analytical tables
            pl.col("PATIENT_NUM").alias("person_source_value"),
            pl.col("PATIENT_NUM")
            .apply(lambda x: lookup(x, "GENDER", 0))  # type: ignore
            .alias("gender_concept_id"),
            pl.col("PATIENT_NUM")
            .apply(lambda x: lookup(x, "RACE", 0))  # type: ignore
            .alias("race_concept_id"),
            pl.col("PATIENT_NUM")
            .apply(lambda x: lookup(x, "ETHNICITY", 0))  # type: ignore
            .alias("ethnicity_concept_id"),
            #
            # null
            pl.lit(None).alias("birth_datetime"),
            pl.lit(None).alias("provider_id"),
            pl.lit(None).alias("care_site_id"),
            pl.lit(None).alias("gender_source_concept_id"),
            pl.lit(None).alias("race_source_concept_id"),
            pl.lit(None).alias("ethnicity_source_concept_id"),
        ]
    ).drop(old_cols)
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(DEST_DIR / "Person.csv", index=False)


def omop_locations():
    from combine_cohorts import combined

    df = unique_pts(combined)
    address_cols = ["ADDR_LN_1", "ADDR_LN_2", "ADDR_CITY", "ADDR_ST_CD", "ZIP_CD_4"]
    omop = (
        df.select(address_cols + ["PATIENT_NUM"])
        .with_columns(
            [
                pl.concat_str(pl.all().exclude("PATIENT_NUM"), sep=" ").alias(
                    "combined_address"
                )
            ]
        )
        .with_columns(
            [
                #
                # required
                # ... none are required except the id field at the end of this query
                #
                # optional
                pl.col("ADDR_LN_1").alias("address_1"),
                pl.col("ADDR_LN_2").alias("address_2"),
                pl.col("ADDR_CITY").alias("city"),
                pl.col("ADDR_ST_CD").alias("state"),
                pl.col("ZIP_CD_4").alias("zip"),
                pl.col("combined_address").alias("location_source_value"),
                # usa omop code
                pl.lit(42046186).alias("country_concept_id"),
                pl.lit("United States").alias("country_source_value"),
                pl.lit(None).alias("county"),
                pl.lit(None).alias("latitude"),
                pl.lit(None).alias("longitude"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("zip").is_in(["", "XXXXX"]))
                .then(None)
                .otherwise(pl.col("zip"))
                .keep_name()
            ]
        )
        .drop(address_cols + ["combined_address", "PATIENT_NUM"])
        .with_row_count(name="location_id", offset=1)
    )
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(DEST_DIR / "Location.csv", index=False)


def omop_deaths():
    from combine_encounters import combined as df

    omop = (
        df.select(
            ["PATIENT_NUM", "ADMT_DT", pl.col("DISCHRG_DISP_CD_DES").alias("discharge")]
        )
        .filter(pl.col("discharge").str.contains("DEATH"))
        .with_columns(
            [
                #
                # required
                pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
                # default to december 30th because we don't know the exact date
                pl.col("ADMT_DT")
                .dt.year()
                .apply(lambda x: f"{x}-12-30")
                .str.strptime(pl.Date, "%Y-%m-%d")
                .alias("death_date"),
                #
                # optional
                # ehr encounter id
                pl.lit(32827).alias("death_type_concept_id"),
                pl.col("ADMT_DT")
                .dt.year()
                .apply(lambda x: f"{x}-12-30 00:00:00")
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                .alias("death_date"),
                #
                # null
                pl.lit(None).alias("cause_concept_id"),
                pl.lit(None).alias("cause_source_value"),
                pl.lit(None).alias("cause_source_concept_id"),
            ]
        )
        .drop(["PATIENT_NUM", "ADMT_DT"])
    )
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(DEST_DIR / "Death.csv", index=False)


def make_encounter_lookup():
    from combine_encounters import combined as df

    return {
        row[1]: row[0]
        for row in df.select(pl.col("VISIT_NUM").unique())
        .with_row_count(name="encounter_id", offset=1)
        .collect()
        .to_numpy()
    }


visit_num_to_id = make_encounter_lookup()


def make_encounter_date_lookup():
    from combine_encounters import combined as df

    return {
        row["VISIT_NUM"]: row["ADMT_DT"]
        for row in df.select(["VISIT_NUM", "ADMT_DT"]).collect().to_dicts()
    }


encounter_date_lookup = make_encounter_date_lookup()


def omop_encounters():
    from combine_encounters import combined

    # drop bc causes dupes
    df = combined.drop("COHORT")
    old_cols = df.columns
    omop = (
        df.with_columns(
            [
                # convert empty strings to None
                pl.when(pl.col(pl.Utf8).str.lengths() == 0)
                .then(None)
                .otherwise(pl.col(pl.Utf8))
                .keep_name()
            ]
        )
        .with_columns(
            [
                #
                # required
                pl.col("VISIT_NUM")
                .map_dict(visit_num_to_id)
                .alias("visit_occurrence_id"),
                pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
                # outpatient, not always true but want to check formatting
                pl.lit(None).alias("visit_concept_id"),
                # pl.lit(9202).alias("visit_concept_id"),
                pl.col("ADMT_DT").cast(pl.Date).alias("visit_start_date"),
                pl.col("DISCHRG_DT").cast(pl.Date).alias("visit_end_date"),
                # ehr encounter type
                pl.lit(32827).alias("visit_type_concept_id"),
                #
                # optional
                pl.col("ADMT_DT").alias("visit_start_datetime"),
                pl.col("DISCHRG_DT").alias("visit_end_datetime"),
                # inpatient/outpatient/ed
                pl.col("IN_OUT_CD_DES").alias("visit_source_value"),
                # ?these two need work to map to OMOP `concept_ids`
                pl.col("ADMT_SRVC_CD_DES").alias("admitted_from_source_value"),
                pl.col("DISCHRG_DISP").alias("discharged_to_source_value"),
                #
                # null
                pl.lit(None).alias("provider_id"),
                pl.lit(None).alias("care_site_id"),
                pl.lit(None).alias("visit_source_concept_id"),
                pl.lit(None).alias("admitted_from_concept_id"),
                pl.lit(None).alias("discharged_to_concept_id"),
                pl.lit(None).alias("preceding_visit_occurrence_id"),
            ]
        )
        .drop(old_cols)
    )
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(DEST_DIR / "Visit_Occurrence.csv", index=False)


def fetch_icd10_codes():
    console.log("Loading ICD10 codes...")
    icd_lookup: dict[str, int] = {
        row[1]: row[0]
        for row in pl.scan_csv(
            KNOWLEDGE_DIR / "CONCEPT.CSV", low_memory=False, sep="\t"
        )
        .filter(pl.col("vocabulary_id").str.starts_with("ICD10"))
        .select(["concept_id", "concept_code"])
        .collect()
        .to_numpy()
    }
    console.log("Loaded ICD10 codes")
    console.log("Loading ICD10 relationships...")
    icd_relationship = {
        row[0]: row[1]
        for row in pl.scan_csv(
            KNOWLEDGE_DIR / "CONCEPT_RELATIONSHIP.CSV", low_memory=False, sep="\t"
        )
        .filter(pl.col("relationship_id").is_in(["Maps to", "Maps to value"]))
        .select(["concept_id_1", "concept_id_2"])
        .collect()
        .to_numpy()
    }
    console.log("Loaded ICD10 relationships")
    console.log("Mapping ICD10 codes to OMOP...")
    icd_to_omop = {
        k: icd_relationship[v] for k, v in icd_lookup.items() if v in icd_relationship
    }
    console.log("Mapped ICD10 codes to OMOP")
    return icd_to_omop


def omop_diagnoses():
    from combine_diagnoses import combined as df

    # this is going to be some lookup from icd10 to either snomed or omop
    icd_lookup = fetch_icd10_codes()

    old_cols = df.columns
    omop = (
        df.with_columns(
            [
                #
                # required
                pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
                pl.col("DIAGNOSIS").map_dict(icd_lookup).alias("condition_concept_id"),
                pl.col("VISIT_NUM")
                .map_dict(encounter_date_lookup)
                .alias("condition_start_date"),
                pl.lit(32827).alias("condition_type_concept_id"),  # EHR encounter
                # optional
                pl.when(pl.col("DIAG_SEQUENCE_NUM") == "1")
                .then(32902)  # primary diagnosis
                .otherwise(None),
                pl.lit(None).alias("condition_start_datetime"),
                pl.lit(None).alias("condition_end_date"),
                pl.lit(None).alias("condition_end_datetime"),
                # null
                pl.lit(None).alias("condition_status_concept_id"),
                pl.lit(None).alias("stop_reason"),
                pl.lit(None).alias("provider_id"),
                pl.col("VISIT_NUM")
                .map_dict(visit_num_to_id)
                .alias("visit_occurrence_id"),
                pl.lit(None).alias("visit_detail_id"),
                pl.col("DIAGNOSIS").alias("condition_source_value"),
                pl.lit(None).alias("condition_source_concept_id"),
                pl.lit(None).alias("condition_status_source_value"),
            ]
        )
        .drop(old_cols)
        .with_row_count(name="condition_occurrence_id", offset=1)
    )
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(
        DEST_DIR / "Condition_occurrence.csv", index=False
    )


def omop_procedures():
    from combine_procedures import combined as df

    console.log("Loading CPT4 codes...")
    cpt4_lookup: dict[str, int] = {
        row[1]: row[0]
        for row in pl.scan_csv(
            KNOWLEDGE_DIR / "CONCEPT_CPT4.CSV", low_memory=False, sep="\t"
        )
        .filter(pl.col("standard_concept") == "S")
        .filter(pl.col("domain_id") == "Procedure")
        # .filter(pl.col("vocabulary_id") == "CPT4")  # this is bc we know but doesn't all valid lookups
        .select(["concept_id", "concept_code"]).collect().to_numpy()
    }
    console.log("Loaded CPT4 codes")

    console.log("Loading CPT4 Modifiers")
    cpt4_mod_lookup: dict[str, int] = {
        row[1]: row[0]
        for row in pl.scan_csv(
            KNOWLEDGE_DIR / "CONCEPT_CPT4.CSV", low_memory=False, sep="\t"
        )
        .filter(pl.col("standard_concept") == "S")
        .filter(pl.col("domain_id") == "Procedure")
        .filter(pl.col("concept_class_id") == "CPT4 Modifier")
        .select(["concept_id", "concept_code"])
        .collect()
        .to_numpy()
    }
    console.log("Loaded CPT4 Modifiers")

    old_cols = df.columns

    omop = (
        df.with_columns(
            [
                #
                # required
                pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
                pl.col("CPT_CODE").map_dict(cpt4_lookup).alias("procedure_concept_id"),
                pl.col("SERVICE_DATE").alias("procedure_date"),
                # 32827 is EHR encounter
                pl.lit(32827).alias("procedure_type_concept_id"),
                #
                # optional
                pl.col("CPT_QUANTITY").cast(pl.Float64).alias("quantity"),
                pl.col("VISIT_NUM")
                .map_dict(visit_num_to_id)
                .alias("visit_occurrence_id"),
                pl.col("CPT_CODE").alias("procedure_source_value"),
                # omop says ETL should decide on method if more than one
                # this insinuates not to keep all, so we just keep the first :)
                pl.col("CPT_MODIFIERS")
                .str.split(",")
                .arr.first()
                .map_dict(cpt4_mod_lookup)
                .alias("modifier_concept_id"),
                pl.col("CPT_MODIFIERS").alias("modifier_source_value"),
                #
                # null
                pl.lit(None).alias("procedure_datetime"),
                pl.lit(None).alias("procedure_end_date"),
                pl.lit(None).alias("procedure_end_datetime"),
                pl.lit(None).alias("visit_detail_id"),
                pl.lit(None).alias("provider_id"),
                # basically told not to use next one
                pl.lit(None).alias("procedure_source_concept_id"),
            ]
        )
        .drop(old_cols)
        .with_row_count(name="procedure_occurrence_id", offset=1)
    )
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(
        DEST_DIR / "Procedure_occurrence.csv", index=False
    )


def omop_labs():
    from combine_labs import combined as df

    # going to need to import concepts
    console.log("Loading LOINC codes...")
    loinc_lookup: dict[str, int] = {
        row[1]: row[0]
        for row in pl.scan_csv(
            KNOWLEDGE_DIR / "CONCEPT.CSV", low_memory=False, sep="\t"
        )
        .filter(pl.col("standard_concept") == "S")
        .filter(pl.col("domain_id") == "Measurement")
        .filter(pl.col("vocabulary_id") == "LOINC")
        .select(["concept_id", "concept_code"])
        .collect()
        .to_numpy()
    }
    console.log("Loaded LOINC codes")
    console.log("Loading units...")
    units_lookup: dict[str, int] = {
        row[1]: row[0]
        for row in pl.scan_csv(
            KNOWLEDGE_DIR / "CONCEPT.CSV", low_memory=False, sep="\t"
        )
        .filter(pl.col("standard_concept") == "S")
        .filter(pl.col("domain_id") == "Unit")
        .select(["concept_id", "concept_code"])
        .collect()
        .to_numpy()
    }
    console.log("Loaded units")

    old_cols = df.columns

    omop = (
        df.with_columns(
            [
                #
                # required
                pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
                pl.col("LOINC_CD")
                .map_dict(loinc_lookup)
                .alias("measurement_concept_id"),
                pl.col("ORDR_PERFRMD_DT_TM").cast(pl.Date).alias("measurement_date"),
                # ehr encounter
                pl.lit(32827).alias("measurement_type_concept_id"),
                #
                # optional
                # this is a case where we have the full datetime so we can extract the next two fields
                # datetime will be required in CDMv6
                pl.col("ORDR_PERFRMD_DT_TM").alias("measurement_datetime"),
                pl.col("ORDR_PERFRMD_DT_TM").cast(pl.Time).alias("measurement_time"),
                pl.lit("=").alias("operator_concept_id"),
                pl.col("VALUE_NUM").alias("value_as_number"),
                pl.col("REFERENCE_LOWER_LIMIT").alias("range_low"),
                pl.col("REFERENCE_UPPER_LIMIT").alias("range_high"),
                pl.col("VISIT_NUM")
                .map_dict(visit_num_to_id)
                .alias("visit_occurrence_id"),
                pl.col("LOINC_CD").alias("measurement_source_value"),
                pl.col("UNIT_OF_MEAS")
                .map_dict(units_lookup)
                .alias("unit_source_concept_id"),
                pl.col("UNIT_OF_MEAS").alias("unit_source_value"),
                #
                # null
                pl.lit(None).alias("value_as_concept_id"),
                pl.lit(None).alias("provider_id"),
                pl.lit(None).alias("visit_detail_id"),
                pl.lit(None).alias("measurement_source_concept_id"),
                pl.lit(None).alias("measurement_event_id"),
                pl.lit(None).alias("meas_event_field_concept_id"),
            ]
        )
        .with_columns([])
        .drop(old_cols)
        .with_row_count(name="measurement_id", offset=1)
    )
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(DEST_DIR / "Measurement.csv", index=False)


def map_routes(x: str) -> int | None:
    if any(
        item in x for item in ["oral", "swish", "spit", "swallow", "mouth", "throat"]
    ):
        return 4132161
    elif "intravenous" in x:
        return 4171047
    elif "subcutaneous" in x:
        return 4142048
    elif "topical" in x or "external" in x:
        return 4263689
    elif "tube" in x:
        return 3661892
    elif "sublingual" in x:
        return 4292110
    elif "transdermal" in x:
        return 4262099
    elif x == "nebulization":
        return 45956874
    elif "nasal" in x or "nostril" in x:
        return 4262914
    elif "external" in x:
        return 4157760
    elif "intravenous" in x:
        return 4171047
    elif "muscular" in x:
        return 4302612
    elif "epidural" in x:
        return 4225555
    elif "inhalation" in x:
        return 45956874
    # there are more, but these are the MAJOR categories
    else:
        return None


# use this in omop MEDS
def omop_emars() -> pl.LazyFrame:
    from combine_emars import combined as emars

    drug_name_lookup: dict[str, int] = {
        row[1]: row[0]
        for row in pl.scan_csv(
            KNOWLEDGE_DIR / "CONCEPT.CSV", low_memory=False, sep="\t"
        )
        .filter(pl.col("standard_concept") == "S")
        .filter(pl.col("domain_id") == "Drug")
        .select(["concept_id", "concept_code"])
        .collect()
        .to_numpy()
    }

    old_emar_cols = emars.columns
    table = (
        emars.with_columns(
            [
                # convert empty strings to None
                pl.when(pl.col(pl.Utf8).str.lengths() == 0)
                .then(None)
                .otherwise(pl.col(pl.Utf8))
                .keep_name()
            ]
        )
        .with_columns(
            [
                #
                # required
                pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
                pl.col("MED_ORDER_NAME")
                .map_dict(drug_name_lookup)
                .alias("drug_concept_id"),
                pl.col("MED_ADMINISTERED_DTTM")
                .cast(pl.Date)
                .alias("drug_exposure_start_date"),
                pl.col("STOP_DATETIME").cast(pl.Date).alias("drug_exposure_end_date"),
                # ehr prescription encounter
                pl.lit(32838).alias("drug_type_concept_id"),
                #
                # optional
                pl.col("MED_ADMINISTERED_DTTM").alias("drug_exposure_start_datetime"),
                pl.col("STOP_DATETIME").alias("drug_exposure_end_datetime"),
                pl.col("STOP_DATETIME").alias("verbatim_end_date"),
                pl.col("DISCONTINUE_RSN").alias("stop_reason"),
                pl.lit("").alias("refills"),  # None for EMARS
                pl.lit("DOSE").alias("quantity"),
                pl.lit("1").alias("days_supply"),  # default
                pl.col("SUMMARY_LINE").alias("sig"),
                pl.col("ORDER_ROUTE_CODE")
                .str.to_lowercase()
                .apply(map_routes)
                .alias("route_concept_id"),
                pl.col("VISIT_NUM")
                .map_dict(visit_num_to_id)
                .alias("visit_occurrence_id"),
                pl.lit("MED_ORDER_NAME").alias("drug_source_value"),
                pl.col("ORDER_ROUTE_CODE").alias("route_source_value"),
                pl.col("DOSEUNIT").alias("dose_unit_source_value"),
                pl.col("PRIMARY_NDC").alias("drug_source_concept_id"),
                #
                # null
                pl.lit("").alias("provider_id"),
                pl.lit("").alias("lot_number"),
                pl.lit("").alias("visit_detail_id"),
            ]
        )
        .drop(old_emar_cols)
    )
    console.log("EMAR done")
    return table


# use this in omop MEDS
def omop_rx() -> pl.LazyFrame:
    from combine_rx import combined as rx

    ndc_lookup: dict[str, int] = {
        row[1]: row[0]
        for row in pl.scan_csv(
            KNOWLEDGE_DIR / "CONCEPT.CSV", low_memory=False, sep="\t"
        )
        .filter(pl.col("standard_concept") == "S")
        .filter(pl.col("domain_id") == "Drug")
        .select(["concept_id", "concept_code"])
        .collect()
        .to_numpy()
    }

    old_rx_cols = rx.columns
    table = (
        rx.with_columns(
            [
                # convert empty strings to None
                pl.when(pl.col(pl.Utf8).str.lengths() == 0)
                .then(None)
                .otherwise(pl.col(pl.Utf8))
                .keep_name()
            ]
        )
        .with_columns(
            [
                #
                # required
                pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
                pl.col("NDC").map_dict(ndc_lookup).alias("drug_concept_id"),
                pl.col("ORDER_START_DTTM")
                .cast(pl.Date)
                .alias("drug_exposure_start_date"),
                pl.col("ORDER_STOP_DTTM").cast(pl.Date).alias("drug_exposure_end_date"),
                # ehr prescription
                pl.lit(32838).alias("drug_type_concept_id"),
                #
                # optional
                pl.col("ORDER_START_DTTM").alias("drug_exposure_start_datetime"),
                pl.col("ORDER_STOP_DTTM").alias("drug_exposure_end_datetime"),
                pl.col("ORDER_STOP_DTTM").alias("verbatim_end_date"),
                pl.col("RSN_FOR_DISCON_DESCR").alias("stop_reason"),
                pl.col("REFILLS").alias("refills"),
                pl.col("QUANTITY").alias("quantity"),
                pl.col("DAYS_SUPPLY").alias("days_supply"),
                pl.col("ORDER_SIG").alias("sig"),
                # see above
                pl.col("ROUTE")
                .str.to_lowercase()
                .apply(map_routes)
                .alias("route_concept_id"),
                pl.lit("DOSE").alias("drug_source_value"),
                pl.col("ROUTE").alias("route_source_value"),
                pl.col("DOSE_UOM").alias("dose_unit_source_value"),
                pl.col("NDC").alias("drug_source_concept_id"),
                #
                # null
                pl.lit("").alias("provider_id"),
                pl.lit("").alias("lot_number"),
                pl.lit(0).cast(pl.Int64).alias("visit_occurrence_id"),
                pl.lit("").alias("visit_detail_id"),
            ]
        )
        .drop(old_rx_cols)
    )
    console.log("RX done")
    return table


def omop_medications():
    # not sure why we have to do this this way and why standard `pl.concat` doesn't work
    # but whatever
    emars = omop_emars()
    emars = emars.select(sorted(emars.columns))
    # emars.collect().to_pandas().to_csv("EMAR.csv", index=False)
    rx = omop_rx()
    rx = rx.select(sorted(rx.columns))
    # need these lengths to infer correct schema
    omop = pl.concat(
        [
            emars,
            rx,
        ]
    ).with_row_count(name="drug_exposure_id", offset=1)
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(DEST_DIR / "Drug_Exposure.csv", index=False)


def map_note_type(x: str) -> int | None:
    if x == "2.  intake & output" or "flowsheet" in x:
        return 706274
    elif "progress" in x:
        return 706550
    elif "education" in x:
        return 706287
    elif "care plan" in x:
        return 706300
    elif "evaluation" in x:
        return 706346
    elif "oncology" in x:
        return 706266
    elif "discharge" in x:
        return 706531
    elif "admit" in x or "admission" in x:
        return 706554
    elif "note" in x:
        # generic note
        return 706391
    # specific
    elif x == "mechanical ventilation record":
        return 1002469
    else:
        return None


def omop_notes():
    df = pl.scan_ipc(Path().cwd().parent / "data" / "notes.feather")

    def identify_note_type(x: str) -> int:
        if x == "SCM":
            return 32829
        elif x == "EPIC":
            return 32831
        elif x == "AEHR":
            return 32834
        else:
            raise ValueError(f"Unknown source: {x}")

    old_cols = df.columns

    omop = df.with_columns(
        [
            # required
            # already has `note_id` column as identifier
            pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("person_id"),
            pl.col("CREATED_DTM").cast(pl.Date).alias("note_date"),
            # source of note (ehr note, ehr admin, etc)
            pl.col("NOTE_SOURCE")
            .apply(identify_note_type)
            .alias("note_type_concept_id"),
            pl.col("NOTE_TYPE").str.to_lowercase().alias("note_class_concept_id"),
            pl.col("NOTE_TEXT").alias("note_text"),
            # encoding for the note, only valid is utf-8 id
            pl.lit(32678).alias("encoding_concept_id"),
            # english
            pl.lit(4180186).alias("language_concept_id"),
            #
            # optional
            pl.col("CREATED_DTM").alias("note_datetime"),
            pl.col("VISIT_NUM").map_dict(visit_num_to_id).alias("visit_occurrence_id"),
            # source value that was mapped to note_class_concept_id
            pl.col("NOTE_TYPE").alias("note_source_value"),
            #
            # null
            pl.lit(None).alias("note_title"),
            pl.lit(None).alias("provider_id"),
            pl.lit(None).alias("visit_detail_id"),
            pl.lit(None).alias("note_event_id"),
            pl.lit(None).alias("note_event_field_concept_id"),
        ]
    ).drop([c for c in old_cols if c != "note_id"])
    console.log(omop.columns)
    omop.collect().to_pandas().to_csv(DEST_DIR / "Note.csv", index=False)
    del omop, df


def cui_to_snomed_converter() -> dict[str, str]:
    mapper = {}
    fpath = Path().cwd().parent / "data" / "knowledge_bases" / "MRCONSO.RRF"
    with open(fpath, "r") as f:
        for line in f:
            parts = line.split("|")
            if parts[11] != "SNOMEDCT_US":
                continue
            if parts[6] != "Y":
                continue
            if parts[12] != "PT":
                continue
            if parts[4] != "PF":
                continue
            # at this point we have valid parts
            mapper[parts[0]] = parts[13]
    return mapper


def snomed_to_omop_conveter() -> dict[str, int]:
    return {
        row[6]: row[0]
        for row in pl.read_csv(
            Path().cwd().parent / "data" / "knowledge_bases" / "CONCEPT.csv",
            sep="\t",
        )
        .filter(pl.col("standard_concept") == "S")
        .filter(pl.col("vocabulary_id") == "SNOMED")
        .to_numpy()
    }


def omop_note_nlp():
    data_dir = Path().cwd().parent / "data" / "scispacy_output" / "batches"
    files = list(data_dir.iterdir())

    console.log("[yellow]Creating cui to snomed map")
    cui_to_snomed = cui_to_snomed_converter()
    console.log("[green]Created cui to snomed map")

    console.log("[yellow]Creating snomed to omop map")
    snomed_to_omop = snomed_to_omop_conveter()
    console.log("[green]Created snomed to omop map")

    line_offset = 1

    with open(DEST_DIR / "Note_NLP.csv", "w") as f:
        for i, file in tqdm(enumerate(files), total=len(files)):
            df = (
                pl.read_ipc(file)
                .with_columns(
                    [
                        pl.col("nlp_datetime").str.strptime(
                            pl.Datetime, "%Y-%m-%dT%H:%M:%S%.6f"
                        ),
                        pl.col("cui").map_dict(cui_to_snomed).alias("snomed_id"),
                    ]
                )
                .with_columns(
                    [
                        pl.col("nlp_datetime").cast(pl.Date).alias("nlp_date"),
                        pl.col("snomed_id")
                        .map_dict(snomed_to_omop)
                        .alias("note_nlp_concept_id"),
                    ]
                )
            )
            old_cols = df.columns
            omop = (
                df.with_columns(
                    [
                        #
                        # required
                        # for now this is an old invalid row_id
                        pl.col("row_num").alias("note_id"),
                        # raw text extracted
                        pl.col("entity").alias("lexical_variant"),
                        # date run
                        pl.col("nlp_date"),
                        #
                        # optional
                        pl.col("nlp_datetime"),
                        pl.lit("scispacy v0.5.1").alias("nlp_system"),
                        pl.col("cui").alias("note_nlp_source_concept_id"),
                        pl.col("note_nlp_concept_id"),
                        #
                        # null
                        # can't we fill snippet?
                        pl.lit(None).alias("snippet"),
                        pl.lit(None).alias("offset"),
                        pl.lit(None).alias("section_concept_id"),
                        pl.lit(None).alias("term_exists"),
                        pl.lit(None).alias("term_temporal"),
                        pl.lit(None).alias("term_modifiers"),
                    ]
                )
                .drop([c for c in old_cols if c != "nlp_date" and c != "nlp_datetime"])
                .with_row_count(name="note_nlp_id", offset=line_offset)
            )

            line_offset += len(omop)
            omop.to_pandas().to_csv(f, index=False, header=i == 0)
            console.log(f"[green]Wrote {len(omop)} rows to file")


def omop_observation_period():
    # ! requires all the other files to be done first !
    # here we want to look through the dest dir and since its omop we know there will be
    # "date" in the date columns so we can just search all of them :)
    x = pl.read_csv(DEST_DIR / "Person.csv")["person_id"].unique()
    people_table = pl.DataFrame({"person_id": x})
    not_date_files = {
        "Person.csv",
        "Location.csv",
        "Note_NLP.csv",
        "Cohort.csv",
        "Cohort_Definition.csv",
    }
    for file in DEST_DIR.glob("*.csv"):
        if file.name in not_date_files:
            continue
        console.log(f"Getting observation period data from {file.name}...")
        df = pl.scan_csv(file, try_parse_dates=True)
        date_cols = [c for c in df.columns if "date" in c]
        if len(date_cols) == 0:
            raise ValueError(f"Could not find date columns in {file}")
        pt_dates = (
            df.select(date_cols + ["person_id"])
            .groupby("person_id")
            .agg([pl.min(col) for col in date_cols])
        ).with_columns([pl.col("person_id").cast(pl.Int64)])
        people_table = people_table.join(
            pt_dates.collect(), on="person_id", how="outer"
        )
    console.log("Finding min and max dates...")
    omop = (
        people_table.with_columns(
            [pl.concat_list(pl.col(pl.Date, pl.Datetime)).alias("dates")]
        ).select(
            [
                # all
                # required
                pl.col("person_id"),
                # exact = false allows us to parse only date from datetime
                pl.col("dates")
                .arr.min()
                .cast(pl.Date)
                .alias("observation_period_start_date"),
                pl.col("dates")
                .arr.max()
                .cast(pl.Date)
                .alias("observation_period_end_date"),
                pl.lit(32827).alias("period_type_concept_id"),  # EHR encounter
            ]
        )
    ).with_row_count(name="observation_period_id", offset=1)
    console.log(omop.columns)
    omop.to_pandas().to_csv(DEST_DIR / "Observation_Period.csv", index=False)


def omop_cohort_definition():
    data = [
        {
            "cohort_definition_id": 1,
            "cohort_definition_name": "Stimulant",
            "cohort_definition_description": "Stimulant Use Cohort",
            "definition_type_concept_id": 0,  # TODO: ask Daniel unsure of this one...
            "cohort_definition_syntax": "^F14.|^F15.|^T40.5|^T43.6|^T43.62",
            # concept id for Domain 'Person'
            "subject_concept_id": 1147314,
            # when was this initiated, we set to none to avoid confusion
            "cohort_initiation_date": None,
        },
        {
            "cohort_definition_id": 2,
            "cohort_definition_name": "Opioid",
            "cohort_definition_description": "Opioid Use Cohort",
            "definition_type_concept_id": None,  # TODO: see above
            "cohort_definition_syntax": "F11|^T40.[012346]",
            # concept id for Domain 'Person'
            "subject_concept_id": 1147314,
            # when was this initiated, we set to none to avoid confusion
            "cohort_initiation_date": None,
        },
        {
            "cohort_definition_id": 3,
            "cohort_definition_name": "Both",
            "cohort_definition_description": "Both Stimulant and Opioid Use Cohort",
            "definition_type_concept_id": None,  # TODO: see above
            "cohort_definition_syntax": "^F14.|^F15.|^T40.5|^T43.6|^T43.62|F11|^T40.[012346]",
            # concept id for Domain 'Person'
            "subject_concept_id": 1147314,
            # when was this initiated, we set to none to avoid confusion
            "cohort_initiation_date": None,
        },
    ]
    omop = pl.DataFrame(data)
    console.log(omop.columns)
    omop.write_csv(DEST_DIR / "Cohort_Definition.csv")


def omop_cohorts():
    import build_patient_cohort_map as bpcm

    # re-map from above definition
    cohort_definition_map = {
        "stimulant": 1,
        "opioid": 2,
        "both": 3,
    }
    data = bpcm.run()
    omop = (
        pl.from_dicts(data)
        .with_columns(
            [
                # minimal columns needed here are the cohort_definition_id
                # subject_id (person_id) and cohort_start_date and cohort_end_date
                pl.col("cohort")
                .map_dict(cohort_definition_map)
                .alias("cohort_definition_id"),
                pl.col("PATIENT_NUM").map_dict(patient_num_to_id).alias("subject_id"),
                pl.lit("2017-01-01")
                .str.strptime(pl.Date, "%Y-%m-%d")
                .alias("cohort_start_date"),
                pl.lit("2020-08-31")
                .str.strptime(pl.Date, "%Y-%m-%d")
                .alias("cohort_end_date"),
            ]
        )
        .drop(["PATIENT_NUM"])
    )
    console.log(omop.columns)
    omop.write_csv(DEST_DIR / "Cohort.csv")


if __name__ == "__main__":
    for name, func in [
        ("persons", omop_persons),
        ("encounters", omop_encounters),
        ("locations", omop_locations),
        ("diagnoses", omop_diagnoses),
        ("procedures", omop_procedures),
        ("notes", omop_notes),
        ("note_nlp", omop_note_nlp),
        ("labs", omop_labs),
        ("medications", omop_medications),
        ("deaths", omop_deaths),
        ("observation_period", omop_observation_period),
        ("cohort_definition", omop_cohort_definition),
        ("cohorts", omop_cohorts),
        # ("observations", omop_observations),
    ]:
        console.log(f"[yellow]Processing {name} table...[/yellow]")
        func()
        console.log(f"[green]Exported {name} table.[/green]")

    console.log("[green]Done.[/green]")
