"""Reference mappings used by the Silver layer.

Keys are normalised (lower case, letters and digits only) so that 'Wk 4', 'WEEK-4' and 'wk4'
all land on the same key. Anything that is not in a mapping becomes NULL and is then caught
by the data quality rules, so a new spelling never slips through silently.
"""

GENDER = {
    "m": "M", "male": "M",
    "f": "F", "female": "F",
    # 'u' / 'unknown' are deliberately absent: they become NULL and are filled from the subject's other visits
}

TREATMENT_ARM = {
    "placebo": "Placebo", "plcebo": "Placebo", "pbo": "Placebo", "placebopbo": "Placebo",
    "drugx10mg": "Drug-X 10mg", "drugx10": "Drug-X 10mg",
    "drugx20mg": "Drug-X 20mg", "drugx20": "Drug-X 20mg",
}

VISIT_NAME = {
    "screening": "Screening", "scrn": "Screening",
    "baseline": "Baseline",
    "week4": "Week 4", "wk4": "Week 4",
    "week8": "Week 8", "wk8": "Week 8",
    "week12": "Week 12", "wk12": "Week 12",
}
VISIT_ORDER = ["Screening", "Baseline", "Week 4", "Week 8", "Week 12"]

STUDY_STATUS = {
    "completed": "Completed", "complete": "Completed",
    "withdrawn": "Withdrawn", "withdrew": "Withdrawn",
    "losttofollowup": "Lost to Follow-up", "ltfu": "Lost to Follow-up",
    "ongoing": "Ongoing", "active": "Ongoing",
}

COUNTRY = {
    "in": "India", "india": "India",
    "au": "Australia", "aus": "Australia", "australia": "Australia",
}

# Site reference data: site_id -> (canonical name, country)
SITES = {
    "SITE-101": ("Sunrise Medical Centre, Hyderabad", "India"),
    "SITE-102": ("Lotus Institute of Medical Sciences, New Delhi", "India"),
    "SITE-103": ("Harbourview Hospital, Mumbai", "India"),
    "SITE-104": ("Southern Cross Clinical Research, Chennai", "India"),
    "SITE-105": ("Yarra Valley Medical Research, Melbourne", "Australia"),
    "SITE-106": ("Bayside Health Trials Unit, Melbourne", "Australia"),
}

# Adverse event values that mean "no adverse event"
NO_AE_VALUES = ["", "none", "n/a", "na", "nil", "no", "-"]

AE_SEVERITY = {
    "mild": "Mild", "grade1": "Mild", "1": "Mild",
    "moderate": "Moderate", "mod": "Moderate", "grade2": "Moderate", "2": "Moderate",
    "severe": "Severe", "grade3": "Severe", "3": "Severe",
}

YES_NO = {
    "y": True, "yes": True, "1": True, "true": True,
    "n": False, "no": False, "0": False, "false": False,
}

LBS_PER_KG = 2.20462

# Clinical plausibility limits (placeholders: confirm against the trial protocol)
LIMITS = {
    "age": (18, 100),
    "systolic_bp": (70, 250),
    "diastolic_bp": (40, 150),
    "heart_rate": (30, 220),
    "hba1c_pct": (4.0, 15.0),
    "fasting_glucose_mg_dl": (40, 600),
    "adherence_pct": (0.0, 100.0),
    "bmi": (15.0, 60.0),
    "weight_kg": (30.0, 300.0),
}
STUDY_START = "2025-12-01"
