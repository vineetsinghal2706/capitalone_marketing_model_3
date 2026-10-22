# Pre-deployment validation and scoring quality gate

The GitHub Actions workflow only deploys to EC2 after all required gates succeed.

## Automated checks

- **Syntax:** `python -m compileall -q jobs scoring airflow tests`.
- **Required files and smoke tests:** verifies pipeline scripts, DAGs, the scoring module, and gate scripts exist. Imports the scorer and checks that extraction, preprocessing, and scoring entrypoints parse their CLI help.
- **Local score regression:** the checked-in golden cases compare deterministic scores, predictions, and probabilities with frozen expected values. These are regression snapshots, not independent model-accuracy certification.
- **S3 scoring quality gate:** reads a reviewed Parquet fixture with exactly 1,000 unique records, the 38 required features, and independent expected outputs. It compares the score, binary prediction, and probability for all 1,000 rows; the report includes ten full input/reference/actual examples.
- **Feature profile and drift:** records missing count/rate, mean, sample standard deviation, minimum, and maximum for all 38 model features, then compares missing-rate, mean, and standard-deviation shifts against a frozen baseline. Any score mismatch, missing/invalid required field, malformed fixture, or threshold violation fails the job and blocks deployment.

## S3 fixture schema

Configure two reviewed S3 objects:

- `sample_1000_with_expected_outputs.parquet`
- `baseline_feature_profile.json`

The Parquet object must contain exactly 1,000 rows with unique `customer_id`, every item in `scoring/score_if_else.py::FEATURE_NAMES`, and these reference columns:

- `expected_score`
- `expected_prediction`
- `expected_probability`

Expected outputs should come from the modeling-team-approved reference implementation or a reviewed, frozen scoring run. If they are generated from the same scorer currently under test, the gate is still useful for regression control but does not independently prove model accuracy.

The baseline JSON includes statistics for each feature. Create it once, review it, and keep it frozen; do not regenerate it during routine CI.

## Configure GitHub Actions

In **GitHub → Settings → Secrets and variables → Actions → Variables**, set:

- `SCORING_TEST_SAMPLE_S3_URI`: full S3 URI of the 1,000-row Parquet fixture.
- `SCORING_TEST_BASELINE_S3_URI`: full S3 URI of the frozen feature-profile JSON.
- `AWS_DEPLOY_ROLE_ARN`: role ARN used by the workflow's GitHub OIDC connection.

The role must be trusted for GitHub OIDC from this repository's `main` branch and have `s3:GetObject` for both objects (plus `kms:Decrypt` if they are encrypted with a customer-managed KMS key). The deployment role still needs its separate SSM permissions.

## Build a fixture from a trusted reference output

If you have an input prefix and a reference scorer output in S3, you can combine them into a stable test fixture:

```bash
python -m pip install -r requirements.txt
python tests/build_scoring_fixture.py \
  --input-s3-prefix "s3://<S3_BUCKET>/processed/marketing_features/<REFERENCE_DATE>/" \
  --reference-output-s3-uri "s3://<S3_BUCKET>/<TRUSTED_REFERENCE_PREFIX>/scored_data.parquet" \
  --output-s3-uri "s3://<S3_BUCKET>/testing/scoring/sample_1000_with_expected_outputs.parquet" \
  --rows 1000
```

The reference output must contain unique `customer_id`, `score`, `prediction`, and `probability` columns from the approved reference scorer. The builder joins by `customer_id`, sorts IDs for a repeatable selection, and stores expected outputs under the required `expected_*` column names.

## Create the baseline once

After checking the fixture and confirming it matches the trusted reference scorer, create and review a baseline:

```bash
python tests/scoring_quality_gate.py \
  --mode create-baseline \
  --sample-s3-uri "s3://<S3_BUCKET>/testing/scoring/sample_1000_with_expected_outputs.parquet" \
  --baseline-out baseline_feature_profile.json \
  --report-path baseline_creation_report.json

aws s3 cp baseline_feature_profile.json \
  "s3://<S3_BUCKET>/testing/scoring/baseline_feature_profile.json" \
  --region us-east-1
```

Default limits are: absolute missing-rate change up to 0.02 (2 percentage points), mean shift up to 0.5 baseline standard deviations, and standard-deviation relative change up to 25%. Adjust them only after reviewing expected natural variability.

The `scoring-quality-report` GitHub Actions artifact is uploaded for each quality-gate run, with feature statistics and ten expected-versus-actual sample cases. Violations appear as GitHub Actions errors, fail the job, and block deployment.

## Scope

The gate checks a frozen pre-deployment fixture and detects score regressions and fixture/profile drift. It does **not** by itself monitor the distribution of each new production batch; production data drift monitoring should compare each run's processed data with an approved reference baseline separately.

## Run local tests

```bash
python -m pip install -r requirements.txt pytest
python -m compileall -q jobs scoring airflow tests
pytest -q
```
