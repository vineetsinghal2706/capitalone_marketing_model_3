"""Unit tests for the pre-deployment 1,000-row scoring quality gate."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scoring.score_if_else import FEATURE_NAMES


MODULE_PATH = Path(__file__).with_name("scoring_quality_gate.py")
SPEC = importlib.util.spec_from_file_location("scoring_quality_gate", MODULE_PATH)
quality_gate = importlib.util.module_from_spec(SPEC)
import sys
sys.modules[SPEC.name] = quality_gate
SPEC.loader.exec_module(quality_gate)


def _frame(rows=1000):
    data = {"customer_id": [f"sample-{i:04d}" for i in range(rows)]}
    for index, feature in enumerate(FEATURE_NAMES):
        data[feature] = [float(index + (i % 25) + 1) for i in range(rows)]
    data["expected_score"] = [500] * rows
    data["expected_prediction"] = [0] * rows
    data["expected_probability"] = [0.4] * rows
    return pd.DataFrame(data)


def test_feature_profile_has_missing_mean_and_sample_std_for_every_feature():
    frame = _frame(20)
    frame.loc[0, "age"] = np.nan
    profile = quality_gate.feature_profile(frame)

    assert set(profile) == set(FEATURE_NAMES)
    assert profile["age"]["missing_count"] == 1
    assert profile["age"]["missing_rate"] == pytest.approx(0.05)
    assert profile["age"]["mean"] is not None
    assert profile["age"]["std"] is not None


def test_drift_check_passes_when_current_profile_matches_baseline():
    frame = _frame(100)
    profile = quality_gate.feature_profile(frame)
    baseline = {
        "features": profile,
        "thresholds": quality_gate.DEFAULT_THRESHOLDS,
    }

    result = quality_gate.compare_drift(profile, baseline)

    assert result["passed"] is True
    assert result["violations"] == []


def test_drift_check_fails_on_missingness_and_mean_shift():
    base = _frame(100)
    baseline = {
        "features": quality_gate.feature_profile(base),
        "thresholds": quality_gate.DEFAULT_THRESHOLDS,
    }

    changed = base.copy()
    changed["age"] = [np.nan] * len(changed)
    profile = quality_gate.feature_profile(changed)
    result = quality_gate.compare_drift(profile, baseline)

    assert result["passed"] is False
    assert any(
        item["feature"] == "age" and item["metric"] == "missing_rate"
        for item in result["violations"]
    )


def test_sample_shape_requires_exactly_1000_records():
    with pytest.raises(quality_gate.GateFailure, match="exactly 1000 rows"):
        quality_gate.validate_sample_shape(_frame(999))


def test_sample_shape_requires_expected_outputs_and_unique_customer_ids():
    frame = _frame(1000)
    frame.loc[1, "customer_id"] = frame.loc[0, "customer_id"]
    with pytest.raises(quality_gate.GateFailure, match="uniquely identify"):
        quality_gate.validate_sample_shape(frame)

    frame = _frame(1000)
    frame = frame.drop(columns=["expected_score"])
    with pytest.raises(quality_gate.GateFailure, match="missing required columns"):
        quality_gate.validate_sample_shape(frame)
