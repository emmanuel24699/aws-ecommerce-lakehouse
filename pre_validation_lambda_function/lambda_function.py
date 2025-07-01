import boto3
import pandas as pd
import json
from io import BytesIO
import os

# --- Configuration ---
# Fetch the State Machine ARN from an environment variable for security
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")

EXPECTED_COLUMNS = {
    "products": ["product_id", "department_id", "department", "product_name"],
    "orders": [
        "order_num",
        "order_id",
        "user_id",
        "order_timestamp",
        "total_amount",
        "date",
    ],
    "order-items": [
        "id",
        "order_id",
        "user_id",
        "days_since_prior_order",
        "product_id",
        "add_to_cart_order",
        "reordered",
        "order_timestamp",
        "date",
    ],
}

s3_client = boto3.client("s3")
stepfunctions_client = boto3.client("stepfunctions")


def lambda_handler(event, context):
    """
    This function validates the schema of an uploaded S3 file.
    - If valid, it triggers the main Step Function ETL pipeline.
    - If invalid, it moves the file to a rejected folder and stops.
    """

    # Initialize variables to ensure they exist for logging, even in case of early failure
    bucket = None
    key = None

    try:
        # 1. Get file details from the event. This is a common failure point if the event is malformed.
        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]
        print(f"Processing file: s3://{bucket}/{key}")

        # 2. Determine file type from key
        file_type = None
        if "/products/" in key:
            file_type = "products"
        elif "/orders/" in key:
            file_type = "orders"
        elif "/order-items/" in key:
            file_type = "order-items"

        if not file_type:
            print(f"File {key} is in an unrecognized folder. Skipping.")
            return {"statusCode": 400, "body": "Unrecognized file type."}

        # 3. Read header and validate schema
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)

        if key.endswith(".csv"):
            header_df = pd.read_csv(BytesIO(s3_object["Body"].read()), nrows=0)
        elif key.endswith(".xlsx"):
            header_df = pd.read_excel(
                BytesIO(s3_object["Body"].read()), nrows=0, engine="openpyxl"
            )
        else:
            print(f"Unsupported file extension for {key}. Skipping.")
            return

        actual_columns = [str(col).lower() for col in header_df.columns]
        required_columns = EXPECTED_COLUMNS[file_type]

        # 4. Check if all required columns are present
        if all(col in actual_columns for col in required_columns):
            # 5a. If VALID: Start the Step Function
            print(f"Schema for {key} is VALID. Triggering Step Function.")
            stepfunctions_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN, input=json.dumps(event)
            )
        else:
            # 5b. If INVALID: Move to rejected and stop
            print(
                f"Schema for {key} is INVALID. Columns mismatch. Moving file to rejected folder."
            )
            rejected_key = f"rejected/invalid_schema/{key.split('/')[-1]}"

            copy_source = {"Bucket": bucket, "Key": key}
            s3_client.copy_object(
                CopySource=copy_source, Bucket=bucket, Key=rejected_key
            )
            s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"File moved to s3://{bucket}/{rejected_key}")

    except Exception as e:
        # This block now safely handles any error during the process
        print(f"FATAL: An error occurred processing the file '{key}'. Error: {e}")
        # Optionally move the file to a different rejected folder for fatal errors
        if bucket and key:
            try:
                fatal_error_key = f"rejected/processing_error/{key.split('/')[-1]}"
                s3_client.copy_object(
                    CopySource={"Bucket": bucket, "Key": key},
                    Bucket=bucket,
                    Key=fatal_error_key,
                )
                s3_client.delete_object(Bucket=bucket, Key=key)
                print(
                    f"File that caused fatal error moved to s3://{bucket}/{fatal_error_key}"
                )
            except Exception as move_exc:
                print(
                    f"Could not move file after fatal error. Manual intervention required for {key}. Move Error: {move_exc}"
                )
        return {"statusCode": 500, "body": "Internal server error during validation."}

    return {"statusCode": 200, "body": "Validation and routing completed."}
