"""Build a frozen 1,000-row scoring fixture by joining inputs to trusted outputs.

This is a setup utility, not a routine CI step. Reference output must come from a
reviewed model/reference scorer. Re-using output from the scorer under test only
creates a regression snapshot; it is not independent accuracy validation.
"""
import argparse
import io
import os
from pathlib import Path

import boto3
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scoring.score_if_else import FEATURE_NAMES

ID_COLUMN = "customer_id"
REFERENCE_COLUMNS = {
    "score": "expected_score",
    "prediction": "expected_prediction",
    "probability": "expected_probability",
}


def parse_s3_uri(uri):
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected an s3:// URI, got: {uri}")
    bucket, separator, key = uri[5:].partition("/")
    if not separator or not bucket or not key:
        raise ValueError(f"S3 URI must include bucket and object key: {uri}")
    return bucket, key


def list_parquet_uris(s3, prefix_uri):
    bucket, prefix = parse_s3_uri(prefix_uri)
    if not prefix.endswith("/"):
        prefix += "/"
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].lower().endswith(".parquet"):
                files.append(f"s3://{bucket}/{item['Key']}")
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {prefix_uri}")
    return sorted(files)


def read_parquet_object(s3, uri):
    bucket, key = parse_s3_uri(uri)
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(response["Body"].read()))


def read_parquet_prefix(s3, uri):
    frames = [read_parquet_object(s3, item) for item in list_parquet_uris(s3, uri)]
    return pd.concat(frames, ignore_index=True)


def build_fixture(input_df, reference_df, rows):
    missing_inputs = [c for c in [ID_COLUMN, *FEATURE_NAMES] if c not in input_df.columns]
    if missing_inputs:
        raise ValueError(f"Input Parquet is missing required columns: {missing_inputs}")

    missing_reference = [
        c for c in [ID_COLUMN, *REFERENCE_COLUMNS] if c not in reference_df.columns
    ]
    if missing_reference:
        raise ValueError(
            "Reference scorer output is missing required columns: "
            f"{missing_reference}. Expected score, prediction, and probability."
        )

    inputs = input_df[[ID_COLUMN, *FEATURE_NAMES]].copy()
    reference = reference_df[[ID_COLUMN, *REFERENCE_COLUMNS]].copy()

    if inputs[ID_COLUMN].isna().any():
        raise ValueError("Input customer_id contains null values.")
    if reference[ID_COLUMN].isna().any():
        raise ValueError("Reference output customer_id contains null values.")

    # Normalize only the join keys; preserve the original input ID for the fixture.
    inputs["_fixture_join_id"] = inputs[ID_COLUMN].astype(str)
    reference["_fixture_join_id"] = reference[ID_COLUMN].astype(str)
    if inputs["_fixture_join_id"].duplicated().any():
        raise ValueError("Input feature data has duplicate customer_id values.")
    if reference["_fixture_join_id"].duplicated().any():
        raise ValueError("Reference output has duplicate customer_id values.")

    reference = reference.rename(columns=REFERENCE_COLUMNS)
    merged = inputs.merge(
        reference.drop(columns=[ID_COLUMN]),
        on="_fixture_join_id",
        how="inner",
        validate="one_to_one",
    ).drop(columns=["_fixture_join_id"])

    if len(merged) < rows:
        raise ValueError(
            f"Only {len(merged)} customer IDs match between input and reference output; "
            f"need at least {rows}."
        )

    # Sort by the string form of customer_id for repeatable sample selection.
    merged["_sort_id"] = merged[ID_COLUMN].astype(str)
    fixture = (
        merged.sort_values("_sort_id", kind="mergesort")
        .head(rows)
        .drop(columns=["_sort_id"])
        .reset_index(drop=True)
    )
    if fixture[ID_COLUMN].duplicated().any():
        raise ValueError("The selected fixture contains duplicate customer_id values.")
    return fixture


def parse_args():
    parser = argparse.ArgumentParser(
        description="Join processed S3 input features to a trusted scoring output."
    )
    parser.add_argument("--input-s3-prefix", required=True)
    parser.add_argument("--reference-output-s3-uri", required=True)
    parser.add_argument("--output-s3-uri", required=True)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    return parser.parse_args()


def main():
    args = parse_args()
    if args.rows != 1000:
        raise ValueError("This pre-deployment fixture builder requires exactly 1,000 rows.")

    s3 = boto3.client("s3", region_name=args.region)
    inputs = read_parquet_prefix(s3, args.input_s3_prefix)
    reference = read_parquet_object(s3, args.reference_output_s3_uri)
    fixture = build_fixture(inputs, reference, args.rows)

    bucket, key = parse_s3_uri(args.output_s3_uri)
    buffer = io.BytesIO()
    fixture.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Fixture rows: {len(fixture)}")
    print(f"Model input features: {len(FEATURE_NAMES)}")
    print(f"Output fixture: {args.output_s3_uri}")
    print("Reference outputs: expected_score, expected_prediction, expected_probability")


if __name__ == "__main__":
    main()
