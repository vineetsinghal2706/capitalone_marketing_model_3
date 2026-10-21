# Capital One Marketing Model — Local Airflow, EC2 PySpark, No Glue

This repository contains the Capital One marketing data pipeline copied from `capitalone_marketing_model_2`, including the AWS Secrets Manager credential retrieval updates.

## Architecture

Local Airflow orchestrates extraction, preprocessing, and batch scoring on EC2 via AWS Systems Manager (SSM). Snowflake credentials are retrieved at runtime from AWS Secrets Manager by the EC2 instance role. Credentials are not stored in this repository.

## Snowflake Secrets Manager setup

Create an Airflow Variable:
- Key: `snowflake_secret_id`
- Value: the name or ARN of your existing AWS Secrets Manager secret.

The secret's `SecretString` must be a JSON object with Snowflake account/URL, username, password, and warehouse fields. Common lowercase keys and `SNOWFLAKE_*` / Snowflake connector option keys are supported. Example structure only:

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

Grant the EC2 instance role `secretsmanager:GetSecretValue` on the specific secret ARN. If a customer-managed KMS key encrypts the secret, ensure the role has required `kms:Decrypt` permission. The local Airflow AWS identity needs its SSM command/poll permissions but does not need access to the Snowflake secret.

## Runtime and deployment

- EC2 project directory used by the current deployment workflow: `/opt/capitalone_marketing_model`
- Region: `us-east-1`
- Extraction writes to `s3://bank-marketing-ml-poc-vineet/raw/{run_date}/`
- Feature data writes to `s3://bank-marketing-ml-poc-vineet/processed/marketing_features/{run_date}/`
- Scoring output writes to `s3://bank-marketing-ml-poc-vineet/output/model_v1/{run_date}/`

Install dependencies on EC2 with `.venv/bin/python -m pip install --no-cache-dir -r requirements.txt`. Run checks with `.venv/bin/python -m compileall -q jobs scoring airflow` and `.venv/bin/python -m pytest -q`.

See the DAG files and tests for implementation details. Never commit passwords, tokens, or secret values to GitHub.