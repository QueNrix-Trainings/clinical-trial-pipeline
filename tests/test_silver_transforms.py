import datetime as dt

import pytest

from common.silver_transforms import (SILVER_COLUMNS, dedup_latest, standardise, to_silver)
from helpers import bronze_df, one


# ---------------------------------------------------------------- identifiers and categories
@pytest.mark.parametrize("raw", ["SUBJ-1001", "subj-1001", " SUBJ-1001", "SUBJ1001"])
def test_subject_id_variants_become_one_format(spark, raw):
    assert one(spark, subject_id=raw)["subject_id"] == "SUBJ-1001"


def test_unusable_subject_id_becomes_null(spark):
    assert one(spark, subject_id="unknown")["subject_id"] is None


@pytest.mark.parametrize("raw,expected", [("Male", "M"), ("male", "M"), ("m", "M"), ("MALE", "M"),
                                          ("Female", "F"), ("f", "F"), ("FEMALE", "F")])
def test_gender_mapping(spark, raw, expected):
    assert one(spark, gender=raw)["gender"] == expected


@pytest.mark.parametrize("raw,expected", [
    ("PLACEBO", "Placebo"), ("placebo ", "Placebo"), ("Plcebo", "Placebo"),
    ("DRUG-X 10 MG", "Drug-X 10mg"), ("Drug X 10mg", "Drug-X 10mg"), ("drugx_20mg", "Drug-X 20mg")])
def test_treatment_arm_mapping(spark, raw, expected):
    assert one(spark, treatment_arm=raw)["treatment_arm"] == expected


@pytest.mark.parametrize("raw,expected", [
    ("Wk4", "Week 4"), ("WEEK 8", "Week 8"), ("week-12", "Week 12"), ("Scrn", "Screening"),
    ("Base line", "Baseline"), ("BASELINE", "Baseline")])
def test_visit_name_mapping(spark, raw, expected):
    assert one(spark, visit_name=raw)["visit_name"] == expected


def test_unknown_visit_name_becomes_null(spark):
    assert one(spark, visit_name="Week 99")["visit_name"] is None


@pytest.mark.parametrize("raw,expected", [("completed", "Completed"), ("LTFU", "Lost to Follow-up"),
                                          ("Lost to follow up", "Lost to Follow-up"), ("ON-GOING", "Ongoing"),
                                          ("Withdrew", "Withdrawn")])
def test_study_status_mapping(spark, raw, expected):
    assert one(spark, study_status=raw)["study_status"] == expected


def test_site_name_variants_resolve_and_site_id_is_derived(spark):
    r = one(spark, site_id=None, site_name="SUNRISE MEDICAL CENTER - HYDERABAD ", country="IN")
    assert r["site_id"] == "SITE-101"
    assert r["site_name"] == "Sunrise Medical Centre, Hyderabad"
    assert r["country"] == "India"
    assert "site_id_derived_from_name" in r["dq_fixes"]


def test_investigator_name_is_normalised(spark):
    assert one(spark, investigator_name="DR RAMESH IYER ")["investigator_name"] == "Dr. Ramesh Iyer"


# ---------------------------------------------------------------- dates
@pytest.mark.parametrize("raw,expected", [
    ("2026-03-10", dt.date(2026, 3, 10)),
    ("10/03/2026", dt.date(2026, 3, 10)),          # day/month/year
    ("10-Mar-2026", dt.date(2026, 3, 10)),
    ("NA", None), ("2026-02-30", None), ("00/00/0000", None), ("not a date", None)])
def test_visit_date_parsing(spark, raw, expected):
    assert one(spark, visit_date=raw)["visit_date"] == expected


# ---------------------------------------------------------------- numbers
def test_pounds_are_converted_to_kg(spark):
    r = one(spark, weight_value=220.5, weight_unit="LBS")
    assert r["weight_kg"] == pytest.approx(100.0, abs=0.1)
    assert "weight_lbs_to_kg" in r["dq_fixes"]


def test_kg_is_left_alone_whatever_the_spelling(spark):
    assert one(spark, weight_value=80.0, weight_unit="Kgs")["weight_kg"] == 80.0


def test_bmi_is_recomputed_from_kg_and_height(spark):
    r = one(spark, weight_value=80.0, weight_unit="kg", height_cm=180.0, bmi=None)
    assert r["bmi"] == 24.7
    assert "bmi_recomputed" in r["dq_fixes"]


def test_bmi_computed_from_pounds_by_the_source_is_corrected(spark):
    r = one(spark, weight_value=176.4, weight_unit="lbs", height_cm=180.0, bmi=54.4)
    assert r["bmi"] == pytest.approx(24.7, abs=0.1)


def test_swapped_blood_pressure_is_corrected(spark):
    r = one(spark, systolic_bp=93, diastolic_bp=139)
    assert (r["systolic_bp"], r["diastolic_bp"]) == (139, 93)
    assert "bp_swapped" in r["dq_fixes"]


def test_implausible_blood_pressure_is_not_swapped(spark):
    r = one(spark, systolic_bp=0, diastolic_bp=92)
    assert (r["systolic_bp"], r["diastolic_bp"]) == (0, 92)      # left for the DQ rules to quarantine


def test_impossible_age_is_nulled_then_refilled_from_the_same_subject(spark):
    out = standardise(bronze_df(spark, dict(age=199, visit_name="Baseline"),
                                dict(age=55, visit_name="Week 4"))).collect()
    ages = {r["visit_name"]: r["age"] for r in out}
    assert ages == {"Baseline": 55, "Week 4": 55}
    baseline = next(r for r in out if r["visit_name"] == "Baseline")
    assert "age_out_of_range_nulled" in baseline["dq_fixes"] and "age_imputed" in baseline["dq_fixes"]


@pytest.mark.parametrize("bad", [105.0, 200.0, -5.0])
def test_adherence_outside_0_100_is_nulled(spark, bad):
    assert one(spark, adherence_pct=bad)["adherence_pct"] is None


def test_missing_gender_is_filled_from_other_visits_else_unknown(spark):
    out = standardise(bronze_df(spark, dict(gender="Female", visit_name="Baseline"),
                                dict(gender=None, visit_name="Week 4"),
                                dict(subject_id="SUBJ-2002", gender="U", visit_name="Baseline"))).collect()
    by = {(r["subject_id"], r["visit_name"]): r["gender"] for r in out}
    assert by[("SUBJ-1001", "Week 4")] == "F"
    assert by[("SUBJ-2002", "Baseline")] == "Unknown"


# ---------------------------------------------------------------- adverse events
@pytest.mark.parametrize("raw", [None, "", "None", "N/A", "NIL", "nil "])
def test_every_spelling_of_no_adverse_event(spark, raw):
    r = one(spark, ae_term=raw, ae_severity="N/A", serious_ae_flag="N")
    assert r["ae_flag"] is False and r["ae_term"] is None and r["ae_severity"] is None and r["serious_ae"] is False


def test_adverse_event_is_normalised(spark):
    r = one(spark, ae_term="UPPER RESPIRATORY INFECTION ", ae_severity="Grade 2", serious_ae_flag="Yes")
    assert r["ae_flag"] is True
    assert r["ae_term"] == "Upper respiratory infection"
    assert r["ae_severity"] == "Moderate"
    assert r["serious_ae"] is True


@pytest.mark.parametrize("raw,expected", [("MILD", "Mild"), ("1", "Mild"), ("MOD", "Moderate"),
                                          ("Grade 3", "Severe"), ("severe", "Severe")])
def test_severity_mapping(spark, raw, expected):
    assert one(spark, ae_term="Nausea", ae_severity=raw)["ae_severity"] == expected


@pytest.mark.parametrize("raw,expected", [("Y", True), ("YES", True), ("1", True), ("No", False), ("0", False)])
def test_serious_flag_mapping(spark, raw, expected):
    assert one(spark, ae_term="Nausea", serious_ae_flag=raw)["serious_ae"] is expected


# ---------------------------------------------------------------- de-duplication and output shape
def test_dedup_keeps_the_latest_version(spark):
    df = bronze_df(spark,
                   dict(hba1c_pct=8.0, modified_date=dt.datetime(2026, 3, 11)),
                   dict(hba1c_pct=7.5, modified_date=dt.datetime(2026, 5, 1)),       # corrected re-send
                   dict(subject_id="subj-1001", hba1c_pct=9.9, modified_date=dt.datetime(2026, 1, 1)))
    out = dedup_latest(standardise(df)).collect()
    assert len(out) == 1 and out[0]["hba1c_pct"] == 7.5


def test_rows_with_a_missing_key_are_never_merged_together(spark):
    df = bronze_df(spark, dict(subject_id=None), dict(subject_id=None), dict(visit_name="nonsense"))
    assert dedup_latest(standardise(df)).count() == 3


def test_to_silver_has_exactly_the_silver_columns(spark):
    from common.dq_checks import apply_rules
    from common.silver_rules import RULES
    out = to_silver(apply_rules(standardise(bronze_df(spark, {})), RULES))
    assert out.columns == SILVER_COLUMNS


def test_raw_record_keeps_the_original_values(spark):
    from common.silver_transforms import standardise as s
    r = s(bronze_df(spark, dict(gender="Male", systolic_bp=999))).collect()[0]
    assert '"gender":"Male"' in r["raw_record"] and '"systolic_bp":999' in r["raw_record"]
