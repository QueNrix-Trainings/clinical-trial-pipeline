import datetime as dt

import pytest

from common.dq_checks import DQLog, apply_rules, split_clean_quarantine, summarise
from common.silver_rules import RULES
from common.silver_transforms import standardise
from helpers import bronze_df


# ---------------------------------------------------------------- the generic framework
def _df(spark):
    return spark.createDataFrame([(1, 10, None), (2, 500, 5), (3, 20, 7)], "id int, a int, b int")


def test_error_and_warn_rules_are_reported_separately(spark):
    rules = [("a_small", "ERROR", "a < 100"), ("b_present", "WARN", "b IS NOT NULL")]
    rows = {r["id"]: r for r in apply_rules(_df(spark), rules).collect()}
    assert rows[1]["dq_errors"] == [] and rows[1]["dq_warnings"] == ["b_present"]
    assert rows[2]["dq_errors"] == ["a_small"] and rows[2]["dq_warnings"] == []
    assert rows[3]["dq_errors"] == [] and rows[3]["dq_warnings"] == []


def test_a_null_result_counts_as_a_failure(spark):
    rows = apply_rules(spark.createDataFrame([(None,)], "a int"), [("a_big", "ERROR", "a > 5")]).collect()
    assert rows[0]["dq_errors"] == ["a_big"]


def test_split_and_summarise(spark):
    rules = [("a_small", "ERROR", "a < 100"), ("b_present", "WARN", "b IS NOT NULL")]
    checked = apply_rules(_df(spark), rules)
    clean, quarantine = split_clean_quarantine(checked)
    assert clean.count() == 2 and quarantine.count() == 1
    s = summarise(checked, rules)
    assert (s["total"], s["error_rows"], s["warn_rows"]) == (3, 1, 1)
    assert s["by_rule"] == [("a_small", "ERROR", 1), ("b_present", "WARN", 1)]


def test_dqlog_statuses_and_failed_errors(spark):
    log = DQLog("run-1", "silver")
    log.add("ok", "ERROR", True, 0, "0 rows")
    log.add("bad", "ERROR", False, 3, "0 rows")
    log.add("soft", "WARN", False, 1, "0 rows")
    log.info("rows_in", 100)
    assert log.failed_errors() == ["bad"]
    df = log.to_df(spark)
    assert df.count() == 4 and "checked_at" in df.columns


# ---------------------------------------------------------------- the clinical trial rules
def _errors_and_warnings(spark, **over):
    checked = apply_rules(standardise(bronze_df(spark, over)), RULES).collect()[0]
    return checked["dq_errors"], checked["dq_warnings"]


def test_a_good_row_passes_every_rule(spark):
    assert _errors_and_warnings(spark) == ([], [])


@pytest.mark.parametrize("over,rule", [
    (dict(systolic_bp=999), "systolic_range"),
    (dict(systolic_bp=0, diastolic_bp=92), "systolic_range"),
    (dict(diastolic_bp=5), "diastolic_range"),
    (dict(heart_rate=400), "heart_rate_range"),
    (dict(hba1c_pct=99.0), "hba1c_range"),
    (dict(hba1c_pct=0.0), "hba1c_range"),
    (dict(fasting_glucose_mg_dl=9999), "glucose_range"),
    (dict(visit_date="NA"), "visit_date_valid"),
    (dict(visit_date="2027-01-15"), "visit_date_valid"),     # in the future
    (dict(visit_date="2025-01-15"), "visit_date_valid"),     # before the study started
    (dict(subject_id="unknown"), "subject_id_format"),
    (dict(visit_name="Week 99"), "visit_name_allowed"),
    (dict(treatment_arm="Mystery drug"), "treatment_arm_known"),
    (dict(site_id=None, site_name="Nowhere Clinic"), "site_known"),
    (dict(modified_date=None), "modified_date_present"),
])
def test_error_rules_quarantine_the_row(spark, over, rule):
    errors, _ = _errors_and_warnings(spark, **over)
    assert rule in errors


@pytest.mark.parametrize("over,rule", [
    (dict(age=None), "age_present"),
    (dict(gender="U"), "gender_known"),
    (dict(ae_term="Nausea", ae_severity=None), "ae_severity_present"),
    (dict(ae_term="Nausea", ae_severity="Mild", serious_ae_flag=None), "serious_ae_present"),
])
def test_warn_rules_flag_but_keep_the_row(spark, over, rule):
    errors, warnings = _errors_and_warnings(spark, **over)
    assert errors == [] and rule in warnings


def test_swapped_blood_pressure_is_fixed_so_it_passes(spark):
    assert _errors_and_warnings(spark, systolic_bp=93, diastolic_bp=139) == ([], [])


def test_missing_vitals_are_acceptable(spark):
    errors, _ = _errors_and_warnings(spark, systolic_bp=None, diastolic_bp=None, heart_rate=None, hba1c_pct=None)
    assert errors == []
