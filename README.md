# Capital One Marketing Model — Local Airflow, EC2 PySpark, No Glue



Local Airflow orchestrates the pipeline. Extraction, preprocessing, and batch scoring run on the EC2 instance via AWS Systems Manager (SSM). Snowflake credentials are retrieved at runtime from AWS Secrets Manager by the EC2 instance role. The local Airflow DAG sends only the secret identifier, not the credentials.

## Architecture

```text
Airflow UI / scheduler on your local computer
  |
  | DAG 1 uses SsmRunCommandOperator
  v
EC2: retrieve Snowflake credentials from Secrets Manager using instance role
  -> jobs/snowflake_to_s3.py (PySpark Snowflake connector)
  -> S3 raw/{run_date}/
  |
  | only after DAG 1 succeeds, trigger DAG 2
  v
EC2: jobs/preprocessing.py (local PySpark feature engineering)
  -> S3 processed/marketing_features/{run_date}/
  |
  | only after DAG 2 succeeds, trigger DAG 3
  v
EC2: jobs/batch_score.py
  -> S3 output/model_v1/{run_date}/
```

No AWS Glue jobs are called. Each SSM task waits for its remote script to finish. Failed extraction prevents DAG 1 from triggering preprocessing; failed preprocessing prevents DAG 2 from triggering scoring.

## 1. Snowflake credentials in AWS Secrets Manager

The extraction job retrieves credentials from your existing AWS Secrets Manager secret at runtime. It no longer reads a Snowflake password from `/etc/capitalone-marketing.env`, and the password is never passed in the SSM command or stored in this repository.

### Secret format

The secret's **SecretString** must be a JSON object. Common lowercase keys or the uppercase `SNOWFLAKE_*` names are supported. Example structure only—keep your real values in Secrets Manager:

```json
{
  "account": "your_snowflake_account_identifier",
  "username": "your_snowflake_username",
  "password": "your_snowflake_password",
  "warehouse": "your_warehouse",
  "database": "INGEST_DATA",
  "schema": "PUBLIC",
  "role": "your_read_role"
}
```

The `account` value may be an account identifier or a Snowflake hostname. An optional `url` key can supply the full host. Do not put actual credentials in README files, GitHub Actions variables, or DAG configuration.

### Configure runtime values in Airflow

In the **local Airflow UI**, open **Admin → Variables** and create these variables:

- **Key:** `snowflake_secret_id`; **Value:** the name or ARN of your existing Secrets Manager secret.
- **Key:** `s3_bucket`; **Value:** the name of your target S3 bucket.

These values are injected at runtime by the DAGs. The bucket name is not hard-coded in the Python jobs or DAG files. The secret identifier is not the password; Snowflake credentials are retrieved by the EC2 instance role.

### EC2 IAM permissions

Attach a least-privilege policy to the EC2 instance role allowing `secretsmanager:GetSecretValue` on that specific secret ARN. If the secret uses a customer-managed KMS key, the role may also need `kms:Decrypt` for that key. Keep the existing S3 permissions for the pipeline. The AWS identity used by local Airflow still needs its existing SSM permissions; it does not need permission to read the Snowflake secret.

Remove the old Snowflake username/password from `/etc/capitalone-marketing.env` once you've confirmed the new path works. If that file is still needed for unrelated settings, do not keep the Snowflake password in it. Rotate the password if it was ever committed to GitHub or otherwise exposed.


## 2. Local Airflow configuration

Install provider packages in the Python environment/container where local Airflow runs. Choose versions compatible with your Airflow version:

```bash
pip install apache-airflow-providers-amazon apache-airflow-providers-standard
```

If your Airflow version includes the standard provider already, keep the compatible installed version rather than blindly upgrading Airflow itself. For Docker-based Airflow, the providers must be installed in the Airflow container/image.

Create or verify the Airflow AWS connection:

- **Connection Id:** `aws_default`
- **Connection Type:** Amazon Web Services
- Configure credentials using an approved local AWS profile/role or the connection's supported authentication fields.
- **Region:** `us-east-1`

The AWS identity used by local Airflow needs permission to send commands to this EC2 managed instance and poll command status (including the required SSM `SendCommand`, `GetCommandInvocation`, and `ListCommandInvocations` access). Scope permissions to the instance and the `AWS-RunShellScript` document where feasible. Never put AWS access keys in DAG code.

## 3. EC2 and network prerequisites

- EC2 instance ID used by the DAGs: `i-06ea3e388448b991c` in `us-east-1`. Update the constants in all three DAGs if your instance changes.
- SSM Agent is running and the instance appears as an online managed node in Systems Manager.
- The EC2 instance role includes SSM managed-instance permissions (commonly `AmazonSSMManagedInstanceCore`) and access to required S3 prefixes.
- EC2 has Python and the project environment at `/opt/capitalone_marketing_model_3/.venv`.
- Java compatible with PySpark 3.5 (Java 11 or 17 are typical choices).
- EC2 has network access to Snowflake on HTTPS/443 and to Maven repositories on first connector resolution, unless connector/JDBC jars are installed in advance.
- EC2 has adequate temporary disk space and RAM for local Spark.
- Snowflake role has warehouse usage, database/schema usage, and SELECT privileges on the source table.

Install/update the EC2 project dependencies when the code changes:

```bash
cd /opt/capitalone_marketing_model_3
.venv/bin/python -m pip install --no-cache-dir -r requirements.txt
.venv/bin/python -m compileall -q jobs scoring airflow
.venv/bin/python -m pytest -q
```

GitHub Actions validates pushes and pull requests. For pushes to `main` or a manually started `workflow_dispatch`, EC2 deployment is allowed only after both the validation job and the 1,000-row S3 scoring quality gate pass. The deploy job also requires the repository variable `AWS_DEPLOY_ROLE_ARN` and a correctly configured GitHub OIDC trust/policy. Pull requests run validation but do not deploy.

## Pre-deployment scoring quality gate

Before a push to `main` can deploy to EC2, the workflow runs the following checks:

1. **Syntax:** `python -m compileall -q jobs scoring airflow tests`.
2. **Required files and smoke checks:** verifies pipeline entrypoints and DAG files exist, the scoring module imports, and all three job entrypoints parse `--help`.
3. **Golden score regression:** compares checked-in sample cases to fixed expected scores/probabilities. These are regression snapshots; independent model-accuracy certification requires outputs from the modeling-team-approved reference scorer.
4. **1,000-row S3 score parity:** reads a Parquet test fixture containing exactly 1,000 unique records, all 38 `FEATURE_NAMES`, and reference `expected_score`, `expected_prediction`, and `expected_probability` columns. It runs the current scorer against all rows and compares every output to the trusted expected values. The report also includes the first ten input/expected/actual cases.
5. **Feature profile and drift gate:** calculates missing count/rate, mean, sample standard deviation, minimum and maximum for every model feature. It compares missingness, mean and standard deviation to a frozen baseline. Any mismatch or threshold violation fails the job and prevents EC2 deployment.

### Configure the S3 fixture and baseline

Put these two reviewed objects in your S3 bucket (example paths only):

- `s3://<S3_BUCKET>/testing/scoring/sample_1000_with_expected_outputs.parquet`
- `s3://<S3_BUCKET>/testing/scoring/baseline_feature_profile.json`

The Parquet fixture must have exactly 1,000 rows and these columns:

- `customer_id`: unique identifier per test row.
- All 38 input features listed in `scoring/score_if_else.py` (`FEATURE_NAMES`).
- `expected_score`, `expected_prediction`, and `expected_probability`: fixed outputs from your approved reference implementation. Do not derive these expected values from the same version being tested if you need independent model-accuracy validation.

In GitHub, open **Settings → Secrets and variables → Actions → Variables** and add:

- `SCORING_TEST_SAMPLE_S3_URI`: the full S3 URI of the 1,000-row Parquet fixture.
- `SCORING_TEST_BASELINE_S3_URI`: the full S3 URI of the frozen feature-profile JSON.

The IAM role referenced by `AWS_DEPLOY_ROLE_ARN` must be allowed to read both S3 objects (`s3:GetObject`; also `kms:Decrypt` if they use a customer-managed KMS key). The GitHub OIDC trust policy must allow the repository's `main` branch to assume that role.

### Create the baseline once

After the test fixture's expected outputs have been checked and approved, generate a baseline profile once:

```bash
python -m pip install -r requirements.txt
python tests/scoring_quality_gate.py \
  --mode create-baseline \
  --sample-s3-uri "s3://<S3_BUCKET>/testing/scoring/sample_1000_with_expected_outputs.parquet" \
  --baseline-out baseline_feature_profile.json \
  --report-path baseline_creation_report.json
```

Review the baseline and report, then upload the approved baseline JSON to the path configured in `SCORING_TEST_BASELINE_S3_URI`:

```bash
aws s3 cp baseline_feature_profile.json \
  "s3://<S3_BUCKET>/testing/scoring/baseline_feature_profile.json" \
  --region us-east-1
```

The baseline contains per-feature missingness/mean/std-dev statistics and default drift thresholds: a maximum missing-rate change of 2 percentage points, a mean movement of 0.5 baseline standard deviations, and a 25% relative standard-deviation change. Adjust them only after reviewing the feature definitions and expected natural variability. Do not regenerate the baseline in routine CI; otherwise drift checks would reset themselves. Every run uploads `scoring-quality-report` as a GitHub Actions artifact, including feature statistics and the ten sample comparisons.

**Scope note:** this pre-deployment check tests a frozen reference fixture and detects changes to that fixture/profile plus scoring regressions. Monitoring distributions in each real production batch is a separate runtime data-monitoring control; a static CI fixture alone cannot detect a change in new daily customer data.

## 4. DAGs and local Airflow folder

- `airflow/dags/01_snowflake_to_s3.py`: invokes extraction on EC2 via SSM and triggers DAG 2 only after success.
- `airflow/dags/02_preprocessing.py`: invokes preprocessing on EC2 via SSM and triggers DAG 3 only after success.
- `airflow/dags/03_batch_scoring.py`: invokes batch scoring on EC2 via SSM.

Because Airflow runs locally, copy these DAG files into the **local Airflow instance's configured DAGs folder** and restart/reload the local scheduler if needed. The GitHub deployment workflow's EC2 copy of DAG files does not update the DAGs folder on your local computer.

All DAGs have `schedule=None`; trigger DAG 1 manually in the local Airflow UI. The date defaults to the DAG logical date, but you can provide `run_date` in the DAG-run configuration.

## 5. S3 output locations

All jobs use the bucket provided through the Airflow Variable `s3_bucket` (exported to each EC2 process as `S3_BUCKET`).

- Raw extraction: `s3://<S3_BUCKET>/raw/{run_date}/`
- Feature data: `s3://<S3_BUCKET>/processed/marketing_features/{run_date}/`
- Scoring output: `s3://<S3_BUCKET>/output/model_v1/{run_date}/`

The scripts clear objects under the matching run-date prefix before publishing new files. Avoid overlapping runs for the same date.

## 6. Source query and connector

The default query in `jobs/snowflake_to_s3.py` is:

```sql
SELECT *
FROM INGEST_DATA.PUBLIC.CUSTOMERS
WHERE CUSTOMER_ID IS NOT NULL
```

Verify the database/schema/table and the columns exist in your Snowflake environment. Override the query using `SNOWFLAKE_QUERY` if required. The default connector Maven coordinate is `net.snowflake:spark-snowflake_2.12:3.1.4-spark_3.5`; confirm compatibility with the installed Spark and Scala versions.

## Troubleshooting

- **DAG fails to import:** check local Airflow provider packages and scheduler logs.
- **SSM command cannot start:** check the local `aws_default` connection's IAM permissions, EC2 managed-node online status, and region/instance ID.
- **Secrets Manager access denied:** verify the Airflow Variable `snowflake_secret_id`, secret region, EC2 instance-role `secretsmanager:GetSecretValue` permission, and KMS decrypt permission if applicable.
- **Snowflake login/network failure:** verify account host, credentials, network policy/private routing/IP allowlisting, warehouse, role and grants.
- **S3 AccessDenied:** check the EC2 instance role and bucket policy.
- **Connector download failure:** check outbound Maven access or preinstall connector/JDBC jars.
- **Disk or memory pressure:** inspect `df -h`, available RAM, and `SPARK_DRIVER_MEMORY`.
