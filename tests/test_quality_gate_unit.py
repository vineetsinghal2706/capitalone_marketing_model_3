"""Unit tests for the local ten-row scoring and feature-profile gate."""
import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scoring.score_if_else import FEATURE_NAMES, score_dataframe

MODULE_PATH = Path(__file__).with_name("scoring_quality_gate.py")
SPEC = importlib.util.spec_from_file_location("scoring_quality_gate", MODULE_PATH)
quality_gate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = quality_gate
SPEC.loader.exec_module(quality_gate)

FIXTURE_PATH = PROJECT_ROOT / "tests" / "fixtures" / "score_parity.json"
BASELINE_PATH = PROJECT_ROOT / "tests" / "fixtures" / "feature_baseline.json"


def _input_frame():
    fixture = quality_gate.load_fixture(FIXTURE_PATH)
    return pd.DataFrame(
        [case["features"] for case in fixture["cases"]],
        columns=FEATURE_NAMES,
    )


def test_fixture_contains_ten_predefined_input_output_cases():
    fixture = quality_gate.load_fixture(FIXTURE_PATH)

    assert len(fixture["cases"]) == 10
    for case in fixture["cases"]:
        assert set(case["features"]) == set(FEATURE_NAMES)
        assert {"score", "prediction", "probability"}.issubset(case["expected"])


def test_feature_profile_has_missing_mean_and_sample_std_for_every_feature():
    frame = _input_frame()
    frame.loc[0, "age"] = np.nan
    profile = quality_gate.feature_profile(frame)

    assert set(profile) == set(FEATURE_NAMES)
    assert profile["age"]["missing_count"] == 1
    assert profile["age"]["missing_rate"] == pytest.approx(0.1)
    assert profile["age"]["mean"] is not None
    assert profile["age"]["std"] is not None


def test_checked_in_feature_profile_matches_frozen_baseline():
    frame = _input_frame()
    profile = quality_gate.feature_profile(frame)
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    result = quality_gate.compare_drift(profile, baseline)

    assert result["passed"] is True
    assert result["violations"] == []
    assert result["checks_run"] == len(FEATURE_NAMES) * 3


def test_drift_check_fails_when_missingness_changes():
    frame = _input_frame()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    frame["age"] = np.nan
    result = quality_gate.compare_drift(
        quality_gate.feature_profile(frame),
        baseline,
    )

    assert result["passed"] is False
    assert any(
        item["feature"] == "age" and item["metric"] == "missing_rate"
        for item in result["violations"]
    )


def test_drift_check_fails_when_mean_moves_beyond_threshold():
    frame = _input_frame()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    frame["age"] = frame["age"] + 100000
    result = quality_gate.compare_drift(
        quality_gate.feature_profile(frame),
        baseline,
    )

    assert result["passed"] is False
    assert any(
        item["feature"] == "age" and item["metric"] == "mean"
        for item in result["violations"]
    )


def test_all_ten_expected_scores_match_the_checked_in_scorer():
    fixture = quality_gate.load_fixture(FIXTURE_PATH)
    frame = pd.DataFrame(
        [case["features"] for case in fixture["cases"]],
        columns=FEATURE_NAMES,
    )
    actual = score_dataframe(frame)

    for index, case in enumerate(fixture["cases"]):
        expected = case["expected"]
        got = actual.iloc[index]
        assert int(got["score"]) == expected["score"], case["name"]
        assert int(got["prediction"]) == expected["prediction"], case["name"]
        assert float(got["probability"]) == pytest.approx(
            expected["probability"],
            abs=fixture["probability_tolerance"],
            rel=0,
        ), case["name"]


def test_fixture_rejects_nine_cases(tmp_path):
    fixture = quality_gate.load_fixture(FIXTURE_PATH)
    fixture["cases"] = fixture["cases"][:9]
    path = tmp_path / "short_fixture.json"
    path.write_text(json.dumps(fixture), encoding="utf-8")

    with pytest.raises(quality_gate.GateFailure, match="exactly 10 cases"):
        quality_gate.load_fixture(path)
