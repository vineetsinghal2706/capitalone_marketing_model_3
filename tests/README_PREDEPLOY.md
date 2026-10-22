# Pre-deployment validation and local scoring quality gate

EC2 deployment is blocked unless the validation job and the local scoring quality gate both pass.

## Automated checks

- **Python syntax:** `python -m compileall -q jobs scoring airflow tests`.
- **Required files and smoke tests:** checks that the DAGs, pipeline scripts, scorer, local fixtures, and quality-gate files exist. It also checks scorer import and CLI entrypoints.
- **Ten fixed input/output cases:** `tests/fixtures/score_parity.json` contains ten named inputs. Each case includes all 38 required features and expected `score`, `prediction`, and `probability`. The current scorer is run against all ten cases.
- **Feature statistics:** missing count/rate, mean, sample standard deviation, minimum, and maximum are computed for each of the 38 features.
- **Profile drift check:** current fixture statistics are compared with `tests/fixtures/feature_baseline.json`. A score mismatch or threshold violation fails the workflow.
- **Report artifact:** `scoring-quality-report` contains expected versus actual outputs for all ten cases, their inputs, feature statistics, and drift-check results.

## Files in the repository

- `tests/fixtures/score_parity.json`: ten committed test inputs and expected outputs.
- `tests/fixtures/feature_baseline.json`: frozen per-feature statistics and thresholds.
- `tests/scoring_quality_gate.py`: local-only quality-gate implementation; no AWS/S3 calls.
- `tests/test_quality_gate_unit.py`: tests for expected results, statistics, drift failures, and fixture structure.

No S3 fixture or additional GitHub Actions variables are required. The separate deployment job still assumes the AWS role configured in `AWS_DEPLOY_ROLE_ARN` to deploy code through SSM.

Default feature-profile thresholds are: missing-rate difference no more than 0.02 (2 percentage points), mean movement no more than 0.5 baseline standard deviations, and standard-deviation relative change no more than 25%. Do not change the baseline or thresholds just to make a failed build pass; review intentional changes first.

## Accuracy and production-drift limits

The committed expected outputs provide deterministic regression coverage. They were calculated from the current checked-in scorer, so they are not independent proof that the scorer matches a separate, modeling-team-approved original model. Replace or supplement them with approved reference outputs when available.

The feature baseline describes only these ten committed test rows. It detects unexpected changes to the test fixture and profile; it does **not** monitor actual production-batch distributions. Runtime data drift detection would need to profile the new processed batch and compare it with a reviewed production reference, separately from deployment CI.

## Run locally

```bash
python -m pip install -r requirements.txt pytest
python -m compileall -q jobs scoring airflow tests
pytest -q
python tests/scoring_quality_gate.py
```
