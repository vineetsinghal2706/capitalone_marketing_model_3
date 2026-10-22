"""Validate a 1,000-row S3 scoring fixture and compare its feature profile to baseline.

The fixture must contain exactly 1,000 rows, the model input features, a unique
customer_id, and independently approved expected_score/expected_prediction/
expected_probability columns. The baseline profile is created once from this
approved fixture and then frozen in S3 for CI validation.
"""
import argparse
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scoring.score_if_else import FEATURE_NAMES, score_dataframe


EXPECTED_ROWS = 1000
ID_COLUMN = "customer_id"
EXPECTED_SCORE_COLUMN = "expected_score"
EXPECTED_PREDICTION_COLUMN = "expected_prediction"
EXPECTED_PROBABILITY_COLUMN = "expected_probability"
OUTPUT_COLUMNS = {
    "score": EXPECTED_SCORE_COLUMN,
    "prediction": EXPECTED_PREDICTION_COLUMN,
    "probability": EXPECTED_PROBABILITY_COLUMN,
}
DEFAULT_THRESHOLDS = {
    # Maximum allowed increase/decrease in missing rate, expressed as a fraction.
    "max_missing_rate_delta": 0.02,
    # Maximum movement in a feature mean, measured in baseline standard deviations.
    "max_mean_shift_in_baseline_std": 0.5,
    # Maximum relative change in feature standard deviation.
    "max_std_relative_change": 0.25,
}
DEFAULT_PROBABILITY_TOLERANCE = 1e-6


class GateFailure(RuntimeError):
    """A data quality, drift, or scoring parity check failed."""


def parse_s3_uri(uri):
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected an s3:// URI, got: {uri}")
    bucket, separator, key = uri[5:].partition("/")
    if not separator or not bucket or not key:
        raise ValueError(f"S3 URI must include both bucket and object key: {uri}")
    return bucket, key


def read_s3_bytes(s3, uri):
    bucket, key = parse_s3_uri(uri)
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def read_sample(s3, uri):
    payload = read_s3_bytes(s3, uri)
    try:
        frame = pd.read_parquet(io.BytesIO(payload))
    except Exception as exc:
        raise GateFailure(
            "Unable to read the scoring sample as Parquet. "
            "Verify the S3 object is a valid Parquet file."
        ) from exc
    return frame


def validate_sample_shape(frame):
    if len(frame) != EXPECTED_ROWS:
        raise GateFailure(
            f"Scoring test sample must contain exactly {EXPECTED_ROWS} rows; "
            f"found {len(frame)}."
        )

    required = [
        ID_COLUMN,
        *FEATURE_NAMES,
        EXPECTED_SCORE_COLUMN,
        EXPECTED_PREDICTION_COLUMN,
        EXPECTED_PROBABILITY_COLUMN,
    ]
    missing_columns = [name for name in required if name not in frame.columns]
    if missing_columns:
        raise GateFailure(
            "Scoring sample is missing required columns: "
            + ", ".join(missing_columns)
        )

    if frame[ID_COLUMN].isna().any():
        raise GateFailure(f"{ID_COLUMN} contains null values.")
    if frame[ID_COLUMN].duplicated().any():
        raise GateFailure(
            f"{ID_COLUMN} must uniquely identify each of the {EXPECTED_ROWS} rows."
        )

    distinct_inputs = int(frame[FEATURE_NAMES].drop_duplicates().shape[0])
    if distinct_inputs < 10:
        raise GateFailure(
            "Scoring sample must contain at least 10 distinct feature vectors; "
            f"found {distinct_inputs}."
        )

    for column in OUTPUT_COLUMNS.values():
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any():
            raise GateFailure(
                f"Expected reference output column {column} contains missing or non-numeric values."
            )

    expected_score = pd.to_numeric(frame[EXPECTED_SCORE_COLUMN], errors="coerce")
    expected_prediction = pd.to_numeric(frame[EXPECTED_PREDICTION_COLUMN], errors="coerce")
    expected_probability = pd.to_numeric(frame[EXPECTED_PROBABILITY_COLUMN], errors="coerce")

    if not expected_score.between(100, 999).all():
        raise GateFailure("expected_score values must be between 100 and 999.")
    if not expected_prediction.isin([0, 1]).all():
        raise GateFailure("expected_prediction values must be 0 or 1.")
    if not expected_probability.between(0, 1).all():
        raise GateFailure("expected_probability values must be between 0 and 1.")

    return distinct_inputs


def feature_profile(frame):
    profile = {}
    row_count = len(frame)

    for feature in FEATURE_NAMES:
        numeric = pd.to_numeric(frame[feature], errors="coerce")
        numeric = numeric.replace([np.inf, -np.inf], np.nan)
        valid = numeric.dropna()

        profile[feature] = {
            "row_count": int(row_count),
            "missing_count": int(numeric.isna().sum()),
            "missing_rate": float(numeric.isna().mean()) if row_count else None,
            "mean": float(valid.mean()) if not valid.empty else None,
            "std": float(valid.std(ddof=1)) if len(valid) >= 2 else None,
            "min": float(valid.min()) if not valid.empty else None,
            "max": float(valid.max()) if not valid.empty else None,
        }

    return profile


def compare_scoring_output(frame, probability_tolerance):
    """Compare generated outputs to trusted expected outputs by customer_id."""
    model_inputs = frame[FEATURE_NAMES].copy()
    scored = score_dataframe(model_inputs)

    actual = pd.DataFrame(
        {
            ID_COLUMN: frame[ID_COLUMN].values,
            "actual_score": scored["score"].values,
            "actual_prediction": scored["prediction"].values,
            "actual_probability": scored["probability"].values,
        }
    )
    expected = pd.DataFrame(
        {
            ID_COLUMN: frame[ID_COLUMN].values,
            "expected_score": pd.to_numeric(frame[EXPECTED_SCORE_COLUMN]).values,
            "expected_prediction": pd.to_numeric(frame[EXPECTED_PREDICTION_COLUMN]).values,
            "expected_probability": pd.to_numeric(frame[EXPECTED_PROBABILITY_COLUMN]).values,
        }
    )

    merged = expected.merge(
        actual,
        on=ID_COLUMN,
        how="outer",
        validate="one_to_one",
        indicator=True,
    )
    unmatched = int((merged["_merge"] != "both").sum())
    score_mismatch = merged["actual_score"].ne(merged["expected_score"])
    prediction_mismatch = merged["actual_prediction"].ne(merged["expected_prediction"])
    probability_delta = (
        merged["actual_probability"].astype(float)
        - merged["expected_probability"].astype(float)
    ).abs()
    probability_mismatch = probability_delta > probability_tolerance

    result = {
        "rows_compared": int(len(merged)),
        "unmatched_customer_ids": unmatched,
        "score_mismatches": int(score_mismatch.sum()),
        "prediction_mismatches": int(prediction_mismatch.sum()),
        "probability_mismatches": int(probability_mismatch.sum()),
        "max_absolute_probability_difference": (
            float(probability_delta.max()) if not probability_delta.empty else None
        ),
        "probability_absolute_tolerance": float(probability_tolerance),
    }
    result["passed"] = (
        len(merged) == EXPECTED_ROWS
        and unmatched == 0
        and result["score_mismatches"] == 0
        and result["prediction_mismatches"] == 0
        and result["probability_mismatches"] == 0
    )
    return result


def create_baseline(frame, sample_uri, thresholds, probability_tolerance):
    return {
        "schema_version": 1,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_sample_s3_uri": sample_uri,
        "row_count": int(len(frame)),
        "key_column": ID_COLUMN,
        "feature_names": list(FEATURE_NAMES),
        "expected_output_columns": OUTPUT_COLUMNS,
        "probability_abs_tolerance": float(probability_tolerance),
        "thresholds": thresholds,
        "features": feature_profile(frame),
        "note": (
            "Frozen profile derived from an approved 1,000-row scoring reference sample. "
            "Do not regenerate as part of routine CI; update only after review and approval."
        ),
    }


def compare_drift(current, baseline):
    baseline_features = baseline.get("features")
    if not isinstance(baseline_features, dict):
        raise GateFailure("Baseline JSON must contain a 'features' object.")

    baseline_names = set(baseline_features)
    expected_names = set(FEATURE_NAMES)
    if baseline_names != expected_names:
        missing = sorted(expected_names - baseline_names)
        extra = sorted(baseline_names - expected_names)
        raise GateFailure(
            f"Baseline feature list does not match the scorer. Missing={missing}; extra={extra}"
        )

    thresholds = baseline.get("thresholds", {})
    try:
        max_missing_delta = float(thresholds["max_missing_rate_delta"])
        max_mean_shift = float(thresholds["max_mean_shift_in_baseline_std"])
        max_std_change = float(thresholds["max_std_relative_change"])
    except (KeyError, TypeError, ValueError) as exc:
        raise GateFailure(
            "Baseline thresholds must define max_missing_rate_delta, "
            "max_mean_shift_in_baseline_std, and max_std_relative_change."
        ) from exc

    checks = []
    violations = []

    for feature in FEATURE_NAMES:
        cur = current[feature]
        ref = baseline_features[feature]

        for metric in ("missing_rate", "mean", "std"):
            if cur.get(metric) is None or ref.get(metric) is None:
                violations.append(
                    {
                        "feature": feature,
                        "metric": metric,
                        "reason": "Missing or undefined current/baseline statistic",
                        "current": cur.get(metric),
                        "baseline": ref.get(metric),
                    }
                )
                continue

        if cur.get("missing_rate") is None or ref.get("missing_rate") is None:
            continue
        missing_delta = abs(float(cur["missing_rate"]) - float(ref["missing_rate"]))
        check = {
            "feature": feature,
            "metric": "missing_rate",
            "current": float(cur["missing_rate"]),
            "baseline": float(ref["missing_rate"]),
            "absolute_delta": missing_delta,
            "threshold": max_missing_delta,
            "passed": missing_delta <= max_missing_delta,
        }
        checks.append(check)
        if not check["passed"]:
            violations.append(check)

        if cur.get("mean") is not None and ref.get("mean") is not None and ref.get("std") is not None:
            baseline_std = abs(float(ref["std"]))
            mean_shift = abs(float(cur["mean"]) - float(ref["mean"]))
            mean_shift_std = (
                0.0 if mean_shift == 0 else mean_shift / max(baseline_std, 1e-12)
            )
            check = {
                "feature": feature,
                "metric": "mean",
                "current": float(cur["mean"]),
                "baseline": float(ref["mean"]),
                "shift_in_baseline_std": float(mean_shift_std),
                "threshold": max_mean_shift,
                "passed": mean_shift_std <= max_mean_shift,
            }
            checks.append(check)
            if not check["passed"]:
                violations.append(check)

        if cur.get("std") is not None and ref.get("std") is not None:
            baseline_std = abs(float(ref["std"]))
            std_delta = abs(float(cur["std"]) - float(ref["std"]))
            relative_change = (
                0.0 if std_delta == 0 else std_delta / max(baseline_std, 1e-12)
            )
            check = {
                "feature": feature,
                "metric": "std",
                "current": float(cur["std"]),
                "baseline": float(ref["std"]),
                "relative_change": float(relative_change),
                "threshold": max_std_change,
                "passed": relative_change <= max_std_change,
            }
            checks.append(check)
            if not check["passed"]:
                violations.append(check)

    return {
        "thresholds": {
            "max_missing_rate_delta": max_missing_delta,
            "max_mean_shift_in_baseline_std": max_mean_shift,
            "max_std_relative_change": max_std_change,
        },
        "checks_run": len(checks),
        "violations": violations,
        "passed": len(violations) == 0,
        "checks": checks,
    }


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate the 1,000-row S3 scoring reference and feature drift."
    )
    parser.add_argument("--mode", choices=("validate", "create-baseline"), default="validate")
    parser.add_argument("--sample-s3-uri", required=True)
    parser.add_argument("--baseline-s3-uri")
    parser.add_argument("--baseline-out")
    parser.add_argument("--report-path", default="artifacts/scoring_quality_report.json")
    parser.add_argument("--expected-rows", type=int, default=EXPECTED_ROWS)
    return parser.parse_args()


def main():
    args = parse_args()
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "github_run_id": os.getenv("GITHUB_RUN_ID"),
        "git_commit": os.getenv("GITHUB_SHA"),
        "sample_s3_uri": args.sample_s3_uri,
        "mode": args.mode,
        "status": "failed",
    }

    exit_code = 1
    try:
        if args.expected_rows != EXPECTED_ROWS:
            raise GateFailure(
                f"This quality gate is configured for exactly {EXPECTED_ROWS} sample rows."
            )

        s3 = boto3.client("s3")
        frame = read_sample(s3, args.sample_s3_uri)
        report["sample_rows"] = int(len(frame))
        distinct_inputs = validate_sample_shape(frame)
        report["distinct_feature_vectors"] = distinct_inputs

        current_stats = feature_profile(frame)
        report["feature_statistics"] = current_stats

        probability_tolerance = DEFAULT_PROBABILITY_TOLERANCE
        if args.mode == "validate":
            if not args.baseline_s3_uri:
                raise GateFailure("--baseline-s3-uri is required in validate mode.")
            baseline_payload = read_s3_bytes(s3, args.baseline_s3_uri)
            try:
                baseline = json.loads(baseline_payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise GateFailure("Baseline S3 object must be valid UTF-8 JSON.") from exc
            if baseline.get("schema_version") != 1:
                raise GateFailure("Unsupported or missing baseline schema_version.")
            if baseline.get("row_count") != EXPECTED_ROWS:
                raise GateFailure(
                    f"Baseline row_count must be {EXPECTED_ROWS}; found {baseline.get('row_count')}."
                )
            probability_tolerance = float(
                baseline.get(
                    "probability_abs_tolerance",
                    DEFAULT_PROBABILITY_TOLERANCE,
                )
            )
            report["baseline_s3_uri"] = args.baseline_s3_uri
        elif not args.baseline_out:
            raise GateFailure("--baseline-out is required in create-baseline mode.")

        scoring = compare_scoring_output(frame, probability_tolerance)
        report["scoring_parity"] = scoring

        # Keep ten explicit expected-vs-actual cases in the report; the parity
        # gate above still checks every one of the 1,000 records.
        preview_inputs = frame[FEATURE_NAMES].head(10)
        preview_scored = score_dataframe(preview_inputs)
        preview_cases = []
        for i in range(min(10, len(frame))):
            preview_cases.append(
                {
                    "customer_id": str(frame.iloc[i][ID_COLUMN]),
                    "expected_score": int(frame.iloc[i][EXPECTED_SCORE_COLUMN]),
                    "actual_score": int(preview_scored.iloc[i]["score"]),
                    "expected_prediction": int(frame.iloc[i][EXPECTED_PREDICTION_COLUMN]),
                    "actual_prediction": int(preview_scored.iloc[i]["prediction"]),
                    "expected_probability": float(frame.iloc[i][EXPECTED_PROBABILITY_COLUMN]),
                    "actual_probability": float(preview_scored.iloc[i]["probability"]),
                }
            )
        report["first_10_reference_cases"] = preview_cases

        if not scoring["passed"]:
            raise GateFailure(
                "Scoring parity failed: "
                f"scores={scoring['score_mismatches']}, "
                f"predictions={scoring['prediction_mismatches']}, "
                f"probabilities={scoring['probability_mismatches']}, "
                f"unmatched_customer_ids={scoring['unmatched_customer_ids']}."
            )

        if args.mode == "create-baseline":
            baseline = create_baseline(
                frame,
                args.sample_s3_uri,
                DEFAULT_THRESHOLDS,
                probability_tolerance,
            )
            write_json(args.baseline_out, baseline)
            report["baseline_output_path"] = args.baseline_out
            report["note"] = (
                "Review and approve this baseline before uploading it to S3. "
                "It must come from a stable sample with independently approved expected outputs."
            )
            report["status"] = "passed"
            exit_code = 0
        else:
            drift = compare_drift(current_stats, baseline)
            report["drift"] = drift
            if not drift["passed"]:
                for item in drift["violations"]:
                    print(
                        "::error::Feature drift: "
                        f"feature={item.get('feature')} "
                        f"metric={item.get('metric')} "
                        f"current={item.get('current')} "
                        f"baseline={item.get('baseline')} "
                        f"threshold={item.get('threshold')} "
                        f"reason={item.get('reason', 'threshold exceeded')}",
                        file=sys.stderr,
                    )
                raise GateFailure(
                    f"Feature drift check failed with {len(drift['violations'])} violation(s)."
                )
            report["status"] = "passed"
            exit_code = 0

    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(f"::error::Scoring quality gate failed: {report['error']}", file=sys.stderr)
    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        try:
            write_json(args.report_path, report)
        except Exception as exc:
            print(
                f"::error::Could not write quality report {args.report_path}: {exc}",
                file=sys.stderr,
            )
            exit_code = 1

    print(json.dumps(
        {
            "status": report["status"],
            "sample_rows": report.get("sample_rows"),
            "distinct_feature_vectors": report.get("distinct_feature_vectors"),
            "scoring_parity": report.get("scoring_parity"),
            "drift_violation_count": len(report.get("drift", {}).get("violations", [])),
            "report_path": args.report_path,
            "error": report.get("error"),
        },
        indent=2,
    ))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
