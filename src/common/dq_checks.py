"""Reusable data quality helpers.

A rule is a tuple (name, severity, sql_expression):
  * severity 'ERROR'  -> the row is quarantined
  * severity 'WARN'   -> the row is kept and the rule name is added to dq_warnings
  * the expression must be TRUE when the row is VALID; NULL counts as a failure, so write
    "col IS NULL OR col BETWEEN a AND b" when a missing value is acceptable.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_rules(df: DataFrame, rules) -> DataFrame:
    """Add dq_errors and dq_warnings (arrays of the rule names each row failed)."""
    def failed(severity):
        checks = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name))
                  for name, sev, expr in rules if sev == severity]
        if not checks:
            return F.array().cast("array<string>")
        return F.filter(F.array(*checks), lambda x: x.isNotNull())
    return df.withColumn("dq_errors", failed("ERROR")).withColumn("dq_warnings", failed("WARN"))


def split_clean_quarantine(df: DataFrame):
    """Rows with no ERROR go on as clean; the rest are quarantined with their reasons."""
    clean = df.filter(F.size("dq_errors") == 0)
    quarantine = df.filter(F.size("dq_errors") > 0)
    return clean, quarantine


def summarise(df: DataFrame, rules) -> dict:
    """One pass over the data: total rows, rows with errors/warnings, and failures per rule."""
    aggs = [F.count(F.lit(1)).alias("_total"),
            F.sum(F.when(F.size("dq_errors") > 0, 1).otherwise(0)).alias("_err_rows"),
            F.sum(F.when(F.size("dq_warnings") > 0, 1).otherwise(0)).alias("_warn_rows")]
    for i, (name, sev, _) in enumerate(rules):
        col = "dq_errors" if sev == "ERROR" else "dq_warnings"
        aggs.append(F.sum(F.when(F.array_contains(F.col(col), name), 1).otherwise(0)).alias(f"r{i}"))
    row = df.agg(*aggs).first()
    return {
        "total": int(row["_total"] or 0),
        "error_rows": int(row["_err_rows"] or 0),
        "warn_rows": int(row["_warn_rows"] or 0),
        "by_rule": [(name, sev, int(row[f"r{i}"] or 0)) for i, (name, sev, _) in enumerate(rules)],
    }


class DQLog:
    """Collects check results and writes them to a dq_results table.
    Status is PASS, FAIL or INFO (INFO = a metric worth keeping, never fails the job)."""

    SCHEMA = ("run_id string, layer string, check_name string, severity string, status string, "
              "metric double, threshold string, details string")

    def __init__(self, run_id: str, layer: str):
        self.run_id, self.layer, self.rows = run_id, layer, []

    def add(self, name, severity, passed, metric=None, threshold="", details=""):
        self.rows.append((self.run_id, self.layer, name, severity, "PASS" if passed else "FAIL",
                          None if metric is None else float(metric), threshold, details))

    def info(self, name, metric, details=""):
        self.rows.append((self.run_id, self.layer, name, "INFO", "INFO", float(metric), "", details))

    def to_df(self, spark) -> DataFrame:
        return spark.createDataFrame(self.rows, self.SCHEMA).withColumn("checked_at", F.current_timestamp())

    def failed_errors(self):
        return [r[2] for r in self.rows if r[4] == "FAIL" and r[3] == "ERROR"]
