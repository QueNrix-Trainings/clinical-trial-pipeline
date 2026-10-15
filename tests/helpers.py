import datetime as dt

BRONZE_DDL = (
    "record_id int, subject_id string, trial_id string, site_id string, site_name string, country string, "
    "investigator_name string, age int, gender string, treatment_arm string, visit_name string, visit_date string, "
    "weight_value double, weight_unit string, height_cm double, bmi double, systolic_bp int, diastolic_bp int, "
    "heart_rate int, hba1c_pct double, fasting_glucose_mg_dl int, adherence_pct double, ae_term string, "
    "ae_severity string, serious_ae_flag string, study_status string, remarks string, "
    "created_date timestamp, modified_date timestamp, _ingest_time timestamp, _source_file string"
)

DEFAULTS = dict(
    record_id=1, subject_id="SUBJ-1001", trial_id="XR-T2D-301", site_id="SITE-101",
    site_name="Sunrise Medical Centre, Hyderabad", country="India", investigator_name="Dr. Ramesh Iyer",
    age=55, gender="M", treatment_arm="Placebo", visit_name="Baseline", visit_date="2026-03-10",
    weight_value=80.0, weight_unit="kg", height_cm=180.0, bmi=24.7, systolic_bp=130, diastolic_bp=85,
    heart_rate=72, hba1c_pct=8.2, fasting_glucose_mg_dl=160, adherence_pct=95.0, ae_term=None,
    ae_severity=None, serious_ae_flag="N", study_status="Ongoing", remarks=None,
    created_date=dt.datetime(2026, 3, 11), modified_date=dt.datetime(2026, 3, 11),
    _ingest_time=dt.datetime(2026, 10, 1), _source_file="f.parquet",
)
COLS = list(DEFAULTS)


def bronze_df(spark, *rows):
    """Build a Bronze-shaped DataFrame. Each row is a dict of overrides on top of DEFAULTS."""
    data = []
    for i, r in enumerate(rows, start=1):
        merged = {**DEFAULTS, "record_id": i, **r}
        data.append(tuple(merged[c] for c in COLS))
    return spark.createDataFrame(data, BRONZE_DDL)


def one(spark, **over):
    """Standardise a single row and return it as a dict."""
    from common.silver_transforms import standardise
    return standardise(bronze_df(spark, over)).collect()[0].asDict()
