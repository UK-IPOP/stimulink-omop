import polars as pl
from rich.console import Console

from paths import DEID_SOURCE_DIR

pl.Config.set_fmt_str_lengths(80)

console = Console(
    color_system="truecolor",
    force_terminal=True,
    force_jupyter=False,
    markup=True,
    emoji=True,
)

ukhc1 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT1_SCM_ENCOUNTER_LDS.csv",
    infer_schema_length=0,
)
ukhc2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_SCM_ENCOUNTER_LDS.csv",
    infer_schema_length=0,
)
epic1 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT1_EPIC_ENCOUNTER_LDS.csv",
    infer_schema_length=0,
)
epic2 = pl.scan_csv(
    DEID_SOURCE_DIR / "EX5765_COHORT2_EPIC_ENCOUNTER_LDS.csv",
    infer_schema_length=0,
)


ukhc = pl.concat([ukhc1, ukhc2])
epic = pl.concat([epic1, epic2])

ukhc = ukhc.rename(
    {
        "AEHR_SMOKING_STATUS": "SMOKING_STATUS",
    }
)

epic = epic.rename(
    {
        "SEX_ASSIGNED_AT_BIRTH": "BIRTH_SEX",
        "CALC_WT_KG": "WT_KG",
        "RACE1": "RACE",
        "SEXUAL_ORIENTATION_LIST": "SEXUAL_ORIENTATION",
    }
)


ukhc = ukhc.with_columns(
    [
        pl.lit("UKHC").alias("DATA_SOURCE"),
        pl.lit("").alias("READMT_30DAY"),
        pl.lit("").alias("READMT_60DAY"),
        pl.lit("").alias("READMT_90DAY"),
        pl.lit("").alias("FINCL_CLASS_1"),
        pl.lit("").alias("DISCHRG_DISP"),
        pl.col("HT_CM").cast(pl.Float64),
        pl.lit("").alias("ILLICIT_DRUG_USE_PAST_YR"),
    ]
)


epic = epic.with_columns(
    [
        pl.lit("EPIC").alias("DATA_SOURCE"),
        (pl.col("CALC_HT_M").cast(pl.Float64) * 100.0).alias("HT_CM"),
        pl.lit("").alias("CENSUS_TRACT"),
        pl.lit("").alias("EDU_LEVEL"),
        pl.lit("").alias("INS_TYPE"),
        pl.lit("").alias("PRONOUNS"),
        pl.lit("").alias("INS_TYPE"),
        pl.lit("").alias("ADMT_SRVC_CD_DES"),
        pl.lit("").alias("DISCHRG_DISP_CD_DES"),
        pl.lit("").alias("TOBACCO_USE_30DAY"),
        pl.lit("").alias("ILLICIT_DRUG_USE_PAST_YR"),
    ]
)


ukhc = ukhc.drop([])
# tobacco_user and smoking_status is all null
epic = epic.drop(["CALC_HT_M", "TOBACCO_USER"])

# reorder columns
ukhc = ukhc.select(sorted(ukhc.columns))
epic = epic.select(sorted(epic.columns))

# combine
combined = pl.concat([ukhc, epic], how="vertical").with_columns(
    [
        pl.col("ADMT_DT").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"),
        pl.col("DISCHRG_DT").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f"),
    ]
)

console.print(combined.fetch().head(2))

console.log("[green]Done.[/green]")
