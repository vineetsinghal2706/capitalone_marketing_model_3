"""Local-only pre-deployment scoring regression and feature-profile gate.

Ten predefined input rows and their expected outputs are stored in
tests/fixtures/score_parity.json. Feature profile statistics are checked against
the committed tests/fixtures/feature_baseline.json. No S3 or AWS access is used.
"""
import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scoring.score_if_else import FEATURE_NAMES, score_dataframe


EXPECTED_CASE_COUNT = 10
DEFAULT_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "score_parity.json"
DEFAULT_BASELINE = PROJECT_ROOT / "tests" / "fixtures" / "feature_baseline.json"
DEFAULT_REPORT = PROJECT_ROOT / "artifacts" / "scoring_quality_report.json"
DEFAULT_PROBABILITY_TOLERANCE = 1e-12
DEFAULT_THRESHOLDS = {
    "max_missing_rate_delta": 0.02,
    "max_mean_shift_in_baseline_std": 0.5,
    "max_std_relative_change": 0.25,
}


class GateFailure(RuntimeError):
    """A fixture, score-regression, or feature-profile check failed."""


def _is_number(value):
    if value is None or isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def load_fixture(path=DEFAULT_FIXTURE):
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GateFailure(f"Unable to read valid fixture JSON from {path}: {exc}") from exc

    cases = payload.get("cases") if isinstance(payload, dict) else None
    if not isinstance(cases, list) or len(cases) != EXPECTED_CASE_COUNT:
        found = len(cases) if isinstance(cases, list) else "not a list"
        raise GateFailure(
            f"Local scoring fixture must contain exactly {EXPECTED_CASE_COUNT} cases; found {found}."
        )

    names = []
    required_expected = {"score", "prediction", "probability"}
    required_features = set(FEATURE_NAMES)

    for index, case in enumerate(cases):
        if not isinstance(case, dict):
            raise GateFailure(f"Fixture case {index + 1} must be a JSON object.")
        name = case.get("name")
        if not isinstance(name, str) or not name.strip():
            raise GateFailure(f"Fixture case {index + 1} has no valid name.")
        names.append(name)

        features = case.get("features")
        if not isinstance(features, dict):
            raise GateFailure(f"Fixture case {name} must contain an input 'features' object.")
        missing = sorted(required_features - set(features))
        extra = sorted(set(features) - required_features)
        if missing or extra:
            raise GateFailure(
                f"Fixture case {name} feature columns mismatch; missing={missing}; extra={extra}."
            )
        for feature, value in features.items():
            if value is not None and not _is_number(value):
                raise GateFailure(
                    f"Fixture case {name}, feature {feature}, must be numeric or null."
                )

        expected = case.get("expected")
        if not isinstance(expected, dict) or not required_expected.issubset(expected):
            raise GateFailure(
                f"Fixture case {name} must include expected score, prediction, and probability."
            )
        if not _is_number(expected["score"]) or not 100 <= float(expected["score"]) <= 999:
            raise GateFailure(f"Fixture case {name} expected score must be between 100 and 999.")
        if not _is_number(expected["prediction"]) or float(expected["prediction"]) not in (0, 1):
            raise GateFailure(f"Fixture case {name} expected prediction must be 0 or 1.")
        if not _is_number(expected["probability"]) or not 0 <= float(expected["probability"]) <= 1:
            raise GateFailure(f"Fixture case {name} expected probability must be between 0 and 1.")

    if len(set(names)) != EXPECTED_CASE_COUNT:
        raise GateFailure("All ten local scoring fixture case names must be unique.")

    return payload


def feature_profile(frame):
    """Calculate missing count/rate, mean, sample std-dev, min and max per feature."""
    profile = {}
    row_count = int(len(frame))

    for feature in FEATURE_NAMES:
        numeric = pd.to_numeric(frame[feature], errors="coerce")
        numeric = numeric.replace([np.inf, -np.inf], np.nan)
        valid = numeric.dropna()
        profile[feature] = {
            "row_count": row_count,
            "missing_count": int(numeric.isna().sum()),
            "missing_rate": float(numeric.isna().mean()) if row_count else None,
            "mean": float(valid.mean()) if not valid.empty else None,
            "std": float(valid.std(ddof=1)) if len(valid) >= 2 else None,
            "min": float(valid.min()) if not valid.empty else None,
            "max": float(valid.max()) if not valid.empty else None,
        }

    return profile


def compare_drift(current, baseline):
    """Compare the local fixture feature profile with the frozen committed baseline."""
    baseline_features = baseline.get("features")
    if not isinstance(baseline_features, dict):
        raise GateFailure("Feature baseline JSON must contain a 'features' object.")

    expected_names = set(FEATURE_NAMES)
    actual_names = set(baseline_features)
    if actual_names != expected_names:
        raise GateFailure(
            "Feature baseline does not match the scorer; "
            f"missing={sorted(expected_names - actual_names)}; "
            f"extra={sorted(actual_names - expected_names)}."
        )

    thresholds = baseline.get("thresholds", {})
    try:
        max_missing_delta = float(thresholds["max_missing_rate_delta"])
        max_mean_shift = float(thresholds["max_mean_shift_in_baseline_std"])
        max_std_change = float(thresholds["max_std_relative_change"])
    except (KeyError, TypeError, ValueError) as exc:
        raise GateFailure(
            "Baseline must define max_missing_rate_delta, "
            "max_mean_shift_in_baseline_std, and max_std_relative_change."
        ) from exc

    checks = []
    violations = []

    for feature in FEATURE_NAMES:
        cur = current[feature]
        ref = baseline_features[feature]

        # Missing or undefined statistics are a quality-gate failure, not a pass.
        for metric in ("missing_rate", "mean", "std"):
            if cur.get(metric) is None or ref.get(metric) is None:
                item = {
                    "feature": feature,
                    "metric": metric,
                    "reason": "Missing or undefined current/baseline statistic",
                    "current": cur.get(metric),
                    "baseline": ref.get(metric),
                }
                violations.append(item)

        if cur.get("missing_rate") is not None and ref.get("missing_rate") is not None:
            delta = abs(float(cur["missing_rate"]) - float(ref["missing_rate"]))
            check = {
                "feature": feature,
                "metric": "missing_rate",
                "current": float(cur["missing_rate"]),
                "baseline": float(ref["missing_rate"]),
                "absolute_delta": delta,
                "threshold": max_missing_delta,
                "passed": delta <= max_missing_delta,
            }
            checks.append(check)
            if not check["passed"]:
                violations.append(check)

        if cur.get("mean") is not None and ref.get("mean") is not None and ref.get("std") is not None:
            mean_delta = abs(float(cur["mean"]) - float(ref["mean"]))
            baseline_std = abs(float(ref["std"]))
            if baseline_std <= 1e-12:
                mean_shift = 0.0 if mean_delta <= 1e-12 else None
                passed = mean_shift is not None
                report_shift = 0.0 if mean_shift == 0.0 else "undefined_for_zero_baseline_std"
            else:
                mean_shift = mean_delta / baseline_std
                passed = mean_shift <= max_mean_shift
                report_shift = float(mean_shift)
            check = {
                "feature": feature,
                "metric": "mean",
                "current": float(cur["mean"]),
                "baseline": float(ref["mean"]),
                "shift_in_baseline_std": report_shift,
                "threshold": max_mean_shift,
                "passed": passed,
            }
            checks.append(check)
            if not passed:
                violations.append(check)

        if cur.get("std") is not None and ref.get("std") is not None:
            cur_std = abs(float(cur["std"]))
            baseline_std = abs(float(ref["std"]))
            if baseline_std <= 1e-12:
                rel_change = 0.0 if cur_std <= 1e-12 else None
                passed = rel_change is not None
                report_change = 0.0 if rel_change == 0.0 else "undefined_for_zero_baseline_std"
            else:
                rel_change = abs(cur_std - baseline_std) / baseline_std
                passed = rel_change <= max_std_change
                report_change = float(rel_change)
            check = {
                "feature": feature,
                "metric": "std",
                "current": float(cur["std"]),
                "baseline": float(ref["std"]),
                "relative_change": report_change,
                "threshold": max_std_change,
                "passed": passed,
            }
            checks.append(check)
            if not passed:
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


def _json_safe(value):
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(_json_safe(payload), indent=2, allow_nan=False),
        encoding="utf-8",
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run local ten-row score regression and feature-profile checks."
    )
    parser.add_argument("--fixture-path", default=str(DEFAULT_FIXTURE))
    parser.add_argument("--baseline-path", default=str(DEFAULT_BASELINE))
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT))
    return parser.parse_args()


def main():
    args = parse_args()
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": __import__("os").getenv("GITHUB_SHA"),
        "fixture_path": str(Path(args.fixture_path)),
        "baseline_path": str(Path(args.baseline_path)),
        "expected_case_count": EXPECTED_CASE_COUNT,
        "status": "failed",
    }
    exit_code = 1

    try:
        fixture = load_fixture(args.fixture_path)
        cases = fixture["cases"]

        baseline = json.loads(Path(args.baseline_path).read_text(encoding="utf-8"))
        if baseline.get("schema_version") != 1:
            raise GateFailure("Unsupported or missing feature baseline schema_version.")
        if baseline.get("row_count") != EXPECTED_CASE_COUNT:
            raise GateFailure(
                f"Baseline must describe {EXPECTED_CASE_COUNT} rows; "
                f"found {baseline.get('row_count')}."
            )
        if baseline.get("feature_names") != list(FEATURE_NAMES):
            raise GateFailure("Baseline feature_names do not match scorer FEATURE_NAMES.")

        inputs = pd.DataFrame(
            [{name: case["features"][name] for name in FEATURE_NAMES} for case in cases],
            columns=FEATURE_NAMES,
        )
        actual = score_dataframe(inputs)
        score_mismatches = []
        probability_tolerance = float(
            fixture.get("probability_tolerance", DEFAULT_PROBABILITY_TOLERANCE)
        )

        sample_cases = []
        for index, case in enumerate(cases):
            expected = case["expected"]
            got = actual.iloc[index]
            score_diff = int(got["score"]) != int(expected["score"])
            prediction_diff = int(got["prediction"]) != int(expected["prediction"])
            probability_delta = abs(float(got["probability"]) - float(expected["probability"]))
            probability_diff = probability_delta > probability_tolerance
            if score_diff or prediction_diff or probability_diff:
                score_mismatches.append(
                    {
                        "case": case["name"],
                        "score_mismatch": score_diff,
                        "prediction_mismatch": prediction_diff,
                        "probability_mismatch": probability_diff,
                        "expected": expected,
                        "actual": {
                            "score": int(got["score"]),
                            "prediction": int(got["prediction"]),
                            "probability": float(got["probability"]),
                        },
                        "absolute_probability_difference": float(probability_delta),
                    }
                )

            input_values = {
                feature: (
                    None
                    if pd.isna(case["features"][feature])
                    else float(case["features"][feature])
                )
                for feature in FEATURE_NAMES
            }
            sample_cases.append(
                {
                    "case": case["name"],
                    "inputs": input_values,
                    "expected": {
                        "score": int(expected["score"]),
                        "prediction": int(expected["prediction"]),
                        "probability": float(expected["probability"]),
                    },
                    "actual": {
                        "score": int(got["score"]),
                        "prediction": int(got["prediction"]),
                        "probability": float(got["probability"]),
                    },
                    "passed": not (score_diff or prediction_diff or probability_diff),
                }
            )

        report["case_count"] = len(cases)
        report["scoring_parity"] = {
            "passed": len(score_mismatches) == 0,
            "cases_compared": len(cases),
            "score_mismatches": sum(item["score_mismatch"] for item in score_mismatches),
            "prediction_mismatches": sum(item["prediction_mismatch"] for item in score_mismatches),
            "probability_mismatches": sum(item["probability_mismatch"] for item in score_mismatches),
            "probability_absolute_tolerance": probability_tolerance,
            "mismatches": score_mismatches,
        }
        report["sample_cases"] = sample_cases
        report["feature_statistics"] = feature_profile(inputs)

        drift = compare_drift(report["feature_statistics"], baseline)
        report["drift"] = drift

        if score_mismatches:
            for item in score_mismatches:
                print(
                    f"::error::Score regression in {item['case']}: "
                    f"expected={item['expected']}, actual={item['actual']}",
                    file=sys.stderr,
                )
        if not drift["passed"]:
            for item in drift["violations"]:
                print(
                    "::error::Feature profile drift: "
                    f"feature={item.get('feature')} metric={item.get('metric')} "
                    f"current={item.get('current')} baseline={item.get('baseline')} "
                    f"threshold={item.get('threshold')} "
                    f"reason={item.get('reason', 'threshold exceeded')}",
                    file=sys.stderr,
                )

        if score_mismatches or not drift["passed"]:
            raise GateFailure(
                f"Quality gate failed: {len(score_mismatches)} score regression(s), "
                f"{len(drift['violations'])} feature-profile violation(s)."
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
            "case_count": report.get("case_count"),
            "scoring_parity": report.get("scoring_parity", {}).get("passed"),
            "drift_violations": len(report.get("drift", {}).get("violations", [])),
            "report_path": args.report_path,
            "error": report.get("error"),
        },
        indent=2,
    ))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
