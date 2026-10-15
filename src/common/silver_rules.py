"""Validation rules applied to STANDARDISED Silver rows (see dq_checks.py for the rule format)."""
from common import mappings as M

L = M.LIMITS


def _between(col, key):
    lo, hi = L[key]
    return f"{col} IS NULL OR {col} BETWEEN {lo} AND {hi}"


def _in_list(values):
    return ", ".join(f"'{v}'" for v in values)


RULES = [
    # ---- ERROR: the row cannot be trusted or identified, so it is quarantined
    ("subject_id_format",   "ERROR", "subject_id RLIKE '^SUBJ-[0-9]+$'"),
    ("visit_name_allowed",  "ERROR", f"visit_name IN ({_in_list(M.VISIT_ORDER)})"),
    ("visit_date_valid",    "ERROR", f"visit_date IS NOT NULL AND visit_date >= DATE'{M.STUDY_START}' AND visit_date <= current_date()"),
    ("treatment_arm_known", "ERROR", f"treatment_arm IN ({_in_list(sorted(set(M.TREATMENT_ARM.values())))})"),
    ("site_known",          "ERROR", "site_id IS NOT NULL"),
    ("modified_date_present", "ERROR", "modified_date IS NOT NULL"),
    ("systolic_range",      "ERROR", _between("systolic_bp", "systolic_bp")),
    ("diastolic_range",     "ERROR", _between("diastolic_bp", "diastolic_bp")),
    ("bp_order",            "ERROR", "systolic_bp IS NULL OR diastolic_bp IS NULL OR systolic_bp > diastolic_bp"),
    ("heart_rate_range",    "ERROR", _between("heart_rate", "heart_rate")),
    ("hba1c_range",         "ERROR", _between("hba1c_pct", "hba1c_pct")),
    ("glucose_range",       "ERROR", _between("fasting_glucose_mg_dl", "fasting_glucose_mg_dl")),
    # ---- WARN: the row is kept and flagged
    ("age_present",         "WARN",  "age IS NOT NULL"),
    ("age_range",           "WARN",  _between("age", "age")),
    ("gender_known",        "WARN",  "gender IN ('M', 'F')"),
    ("weight_range",        "WARN",  _between("weight_kg", "weight_kg")),
    ("bmi_range",           "WARN",  _between("bmi", "bmi")),
    ("adherence_range",     "WARN",  _between("adherence_pct", "adherence_pct")),
    ("study_status_known",  "WARN",  f"study_status IN ({_in_list(sorted(set(M.STUDY_STATUS.values())))})"),
    ("ae_severity_present", "WARN",  "NOT ae_flag OR ae_severity IS NOT NULL"),
    ("serious_ae_present",  "WARN",  "NOT ae_flag OR serious_ae IS NOT NULL"),
]
