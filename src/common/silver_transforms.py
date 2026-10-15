"""Silver-layer transformations for the clinical trial table.

Every function takes a DataFrame and returns a DataFrame, so each step can be unit tested
without touching storage. `dq_fixes` collects the name of every correction applied to a row,
which gives you lineage ("this BMI was recomputed", "this weight was converted from lbs").
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import mappings as M
BRONZE_BUSINESS_COLS = [
    "record_id", "subject_id", "trial_id", "site_id", "site_name", "country", "investigator_name",
    "age", "gender", "treatment_arm", "visit_name", "visit_date", "weight_value", "weight_unit",
    "height_cm", "bmi", "systolic_bp", "diastolic_bp", "heart_rate", "hba1c_pct",
    "fasting_glucose_mg_dl", "adherence_pct", "ae_term", "ae_severity", "serious_ae_flag",
    "study_status", "remarks", "created_date", "modified_date",
]

SILVER_COLUMNS = [
    "subject_id", "trial_id", "visit_name", "visit_date", "site_id", "site_name", "country",
    "investigator_name", "age", "gender", "treatment_arm", "study_status",
    "weight_kg", "height_cm", "bmi", "systolic_bp", "diastolic_bp", "heart_rate",
    "hba1c_pct", "fasting_glucose_mg_dl", "adherence_pct",
    "ae_flag", "ae_term", "ae_severity", "serious_ae", "remarks",
    "source_record_id", "created_date", "modified_date", "dq_fixes", "dq_warnings",
    "_bronze_ingest_time", "_source_file", "_silver_loaded_at",
]


# ----------------------------------------------------------------------------- helpers
def _key(col):
    """Normalised lookup key: lower case, letters and digits only."""
    return F.regexp_replace(F.lower(F.trim(col)), r"[^a-z0-9]", "")


def _map(col, mapping):
    """CASE WHEN lookup that returns NULL for unknown keys (safe under ANSI mode)."""
    key = _key(col)
    expr = None
    for k, v in mapping.items():
        expr = F.when(key == k, F.lit(v)) if expr is None else expr.when(key == k, F.lit(v))
    return expr


def _add_fix(df, cond, name):
    """Append `name` to dq_fixes for rows where `cond` is true. Call BEFORE changing the column."""
    return df.withColumn(
        "dq_fixes",
        F.when(cond, F.concat(F.col("dq_fixes"), F.array(F.lit(name)))).otherwise(F.col("dq_fixes")),
    )


# ----------------------------------------------------------------------------- steps
def add_raw_record(df: DataFrame) -> DataFrame:
    """Keep the untouched source row as JSON, so quarantined rows show what the source really sent."""
    cols = [c for c in BRONZE_BUSINESS_COLS if c in df.columns]
    return (df.withColumn("raw_record", F.to_json(F.struct(*[F.col(c) for c in cols])))
              .withColumn("dq_fixes", F.array().cast("array<string>")))


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim every text column and turn empty strings into NULL."""
    skip = {"raw_record", "_source_file"}
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType) and f.name not in skip:
            c = F.trim(F.col(f.name))
            df = df.withColumn(f.name, F.when(c == "", None).otherwise(c))
    return df


def standardise_identifiers(df: DataFrame) -> DataFrame:
    """SUBJ-1001 / subj-1001 / ' SUBJ-1001' / SUBJ1001 all become SUBJ-1001."""
    digits = F.regexp_extract(F.col("subject_id"), r"(\d+)", 1)
    new_id = F.when(digits != "", F.concat(F.lit("SUBJ-"), digits))
    df = _add_fix(df, F.col("subject_id").isNotNull() & (F.col("subject_id") != new_id), "subject_id_standardised")
    return (df.withColumn("subject_id", new_id)
              .withColumn("trial_id", F.upper(F.col("trial_id"))))


def standardise_categoricals(df: DataFrame) -> DataFrame:
    """Gender, arm, visit name, study status, site, country, investigator."""
    for col, mapping, fix in [
        ("gender", M.GENDER, "gender_standardised"),
        ("treatment_arm", M.TREATMENT_ARM, "treatment_arm_standardised"),
        ("visit_name", M.VISIT_NAME, "visit_name_standardised"),
        ("study_status", M.STUDY_STATUS, "study_status_standardised"),
    ]:
        new = _map(F.col(col), mapping)
        df = _add_fix(df, F.col(col).isNotNull() & new.isNotNull() & (F.col(col) != new), fix)
        df = df.withColumn(col, new)

    # Site: trust a valid site_id, otherwise derive it from the (messy) site name
    site_key = F.regexp_replace(F.regexp_replace(F.lower(F.trim(F.col("site_name"))), "center", "centre"), r"[^a-z0-9]", "")
    name_to_id = {}
    for sid, (name, _) in M.SITES.items():
        k = "".join(ch for ch in name.lower().replace("center", "centre") if ch.isalnum())
        name_to_id[k] = sid
    id_from_name = None
    for k, sid in name_to_id.items():
        id_from_name = F.when(site_key == k, F.lit(sid)) if id_from_name is None else id_from_name.when(site_key == k, F.lit(sid))
    given_id = F.upper(F.trim(F.col("site_id")))
    valid_given = F.when(given_id.isin(*M.SITES.keys()), given_id)
    df = _add_fix(df, valid_given.isNull() & id_from_name.isNotNull(), "site_id_derived_from_name")
    df = df.withColumn("site_id", F.coalesce(valid_given, id_from_name))

    site_name_expr, country_expr = None, None
    for sid, (name, country) in M.SITES.items():
        cond = F.col("site_id") == sid
        site_name_expr = F.when(cond, F.lit(name)) if site_name_expr is None else site_name_expr.when(cond, F.lit(name))
        country_expr = F.when(cond, F.lit(country)) if country_expr is None else country_expr.when(cond, F.lit(country))
    df = df.withColumn("country", F.coalesce(country_expr, _map(F.col("country"), M.COUNTRY)))
    df = df.withColumn("site_name", F.coalesce(site_name_expr, F.col("site_name")))

    # Investigator: 'dr ramesh iyer', 'DR. RAMESH IYER' -> 'Dr. Ramesh Iyer'
    name = F.regexp_replace(F.trim(F.col("investigator_name")), r"(?i)^dr(\.\s*|\s+)", "Dr. ")
    df = df.withColumn("investigator_name", F.initcap(name))
    return df.withColumn("remarks", F.regexp_replace(F.col("remarks"), r"\s+", " "))


def parse_dates(df: DataFrame, formats=("yyyy-MM-dd", "dd/MM/yyyy", "dd-MMM-yyyy")) -> DataFrame:
    """Try each known format; anything that matches none (NA, 2026-02-30, 00/00/0000) becomes NULL.
    Slash dates are day/month/year, the India and Australia convention."""
    parsed = F.coalesce(*[F.expr(f"CAST(try_to_timestamp(visit_date, '{fmt}') AS DATE)") for fmt in formats])
    return (df.withColumn("visit_date", parsed)
              .withColumn("created_date", F.col("created_date").cast("timestamp"))
              .withColumn("modified_date", F.col("modified_date").cast("timestamp")))


def convert_numerics(df: DataFrame) -> DataFrame:
    """Cast types, convert lbs to kg, fix swapped blood pressure, null impossible age/adherence."""
    lo_age, hi_age = M.LIMITS["age"]
    lo_adh, hi_adh = M.LIMITS["adherence_pct"]

    # weight: lbs -> kg
    unit = F.lower(F.trim(F.col("weight_unit")))
    w = F.col("weight_value").cast("double")
    is_lbs = unit.startswith("lb")
    df = _add_fix(df, is_lbs & w.isNotNull(), "weight_lbs_to_kg")
    df = _add_fix(df, unit.isNull() & w.isNotNull(), "weight_unit_assumed_kg")
    weight_kg = (F.when(is_lbs, F.round(w / M.LBS_PER_KG, 1))
                  .when(unit.startswith("kg") | unit.isNull(), F.round(w, 1)))
    df = df.withColumn("weight_kg", weight_kg).drop("weight_value", "weight_unit")

    for c, t in [("age", "int"), ("systolic_bp", "int"), ("diastolic_bp", "int"), ("heart_rate", "int"),
                 ("fasting_glucose_mg_dl", "int"), ("height_cm", "double"), ("bmi", "double"),
                 ("hba1c_pct", "double"), ("adherence_pct", "double")]:
        df = df.withColumn(c, F.col(c).cast(t))
    df = df.withColumn("hba1c_pct", F.round("hba1c_pct", 1))

    # age outside the protocol range is a keying error: null it (it is refilled from the subject's other visits)
    bad_age = F.col("age").isNotNull() & ((F.col("age") < lo_age) | (F.col("age") > hi_age))
    df = _add_fix(df, bad_age, "age_out_of_range_nulled")
    df = df.withColumn("age", F.when(bad_age, None).otherwise(F.col("age")))

    # adherence is a percentage: anything outside 0-100 cannot be trusted
    bad_adh = F.col("adherence_pct").isNotNull() & ((F.col("adherence_pct") < lo_adh) | (F.col("adherence_pct") > hi_adh))
    df = _add_fix(df, bad_adh, "adherence_out_of_range_nulled")
    df = df.withColumn("adherence_pct", F.when(bad_adh, None).otherwise(F.col("adherence_pct")))

    # blood pressure entered the wrong way round: swap only if the swapped values are plausible
    s, d = F.col("systolic_bp"), F.col("diastolic_bp")
    s_lo, s_hi = M.LIMITS["systolic_bp"]
    d_lo, d_hi = M.LIMITS["diastolic_bp"]
    swap = s.isNotNull() & d.isNotNull() & (s < d) & d.between(s_lo, s_hi) & s.between(d_lo, d_hi)
    df = _add_fix(df, swap, "bp_swapped")
    # compute both new values from the ORIGINAL columns first; assigning one at a time would make the
    # second expression see the already-swapped systolic value
    return (df.withColumn("_new_sys", F.when(swap, d).otherwise(s))
              .withColumn("_new_dia", F.when(swap, s).otherwise(d))
              .withColumn("systolic_bp", F.col("_new_sys"))
              .withColumn("diastolic_bp", F.col("_new_dia"))
              .drop("_new_sys", "_new_dia"))


def standardise_adverse_events(df: DataFrame) -> DataFrame:
    """Five spellings of 'no adverse event' collapse into ae_flag = false; severity and seriousness are normalised."""
    term_key = F.lower(F.trim(F.col("ae_term")))
    no_ae = term_key.isNull() | term_key.isin(*M.NO_AE_VALUES)
    ae_flag = ~no_ae
    df = df.withColumn("ae_flag", ae_flag)

    severity = F.when(F.col("ae_flag"), _map(F.col("ae_severity"), M.AE_SEVERITY))
    serious_raw = _map(F.col("serious_ae_flag"), {k: v for k, v in M.YES_NO.items()})
    serious = F.when(~F.col("ae_flag"), F.lit(False)).otherwise(serious_raw)
    sentence_case = F.concat(F.upper(F.substring(term_key, 1, 1)), F.substring(term_key, 2, 200))
    return (df.withColumn("ae_severity", severity)
              .withColumn("serious_ae", serious)
              .withColumn("ae_term", F.when(F.col("ae_flag"), sentence_case))
              .drop("serious_ae_flag"))


def impute_subject_attributes(df: DataFrame, reference: DataFrame = None) -> DataFrame:
    """Subject-level facts (age, gender, height, arm) do not change between visits, so a missing value
    is filled from the same subject's other visits, in this batch first and then from Silver.
    `reference` = one row per subject with ref_age, ref_gender, ref_height_cm, ref_treatment_arm."""
    if reference is not None:
        df = df.join(F.broadcast(reference), "subject_id", "left")
    w = Window.partitionBy("subject_id")
    has_subject = F.col("subject_id").isNotNull()
    for col in ["age", "gender", "height_cm", "treatment_arm"]:
        from_batch = F.when(has_subject, F.max(col).over(w))
        from_ref = F.col(f"ref_{col}") if reference is not None else F.lit(None)
        filled = F.coalesce(F.col(col), from_batch, from_ref)
        df = _add_fix(df, F.col(col).isNull() & filled.isNotNull(), f"{col}_imputed")
        df = df.withColumn(col, filled)
    if reference is not None:
        df = df.drop(*[c for c in df.columns if c.startswith("ref_")])
    return df.withColumn("gender", F.coalesce(F.col("gender"), F.lit("Unknown")))


def derive_bmi(df: DataFrame) -> DataFrame:
    """Never trust the source BMI (some rows were computed from pounds): recompute from kg and cm."""
    computed = F.round(F.col("weight_kg") / F.pow(F.col("height_cm") / 100, 2), 1)
    changed = F.col("bmi").isNull() | (F.abs(F.col("bmi") - computed) > 0.5)
    df = _add_fix(df, computed.isNotNull() & changed, "bmi_recomputed")
    return df.withColumn("bmi", computed)


def standardise(df: DataFrame, reference: DataFrame = None) -> DataFrame:
    """Full standardisation pipeline. Input: Bronze rows. Output: typed, standardised rows (not yet validated)."""
    df = add_raw_record(df)
    df = clean_strings(df)
    df = standardise_identifiers(df)
    df = standardise_categoricals(df)
    df = parse_dates(df)
    df = convert_numerics(df)
    df = standardise_adverse_events(df)
    df = impute_subject_attributes(df, reference)
    return derive_bmi(df)


def dedup_latest(df: DataFrame) -> DataFrame:
    """One row per (subject_id, visit_name): the newest modified_date wins, ties broken by record_id.
    Rows with a missing key get a unique key so they are never merged with each other."""
    def k(col):
        return F.coalesce(F.col(col), F.concat(F.lit("__bad_"), F.col("record_id").cast("string")))
    w = Window.partitionBy(k("subject_id"), k("visit_name")).orderBy(
        F.col("modified_date").desc_nulls_last(), F.col("record_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def to_silver(df: DataFrame) -> DataFrame:
    """Select and order the Silver columns."""
    return (df.withColumnRenamed("record_id", "source_record_id")
              .withColumn("source_record_id", F.col("source_record_id").cast("int"))
              .withColumnRenamed("_ingest_time", "_bronze_ingest_time")
              .withColumn("_silver_loaded_at", F.current_timestamp())
              .select(*SILVER_COLUMNS))
