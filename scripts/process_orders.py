import sys
import boto3
import pandas as pd
from io import BytesIO
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp
from delta.tables import DeltaTable

# --- Initializations ---
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "S3_INPUT_PATH", "S3_PROCESSED_ZONE", "S3_REJECTED_PATH"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Script Parameters ---

s3_input_path = args["S3_INPUT_PATH"]
s3_processed_zone = args["S3_PROCESSED_ZONE"]
s3_rejected_path = args["S3_REJECTED_PATH"]

orders_delta_path = f"{s3_processed_zone}orders/"


# --- Function to Read Multi-Sheet Excel from S3 ---
def read_excel_from_s3(spark_session: SparkSession, file_path: str) -> "DataFrame":
    """Reads all sheets from an Excel file in S3 into a single Spark DataFrame."""
    s3_client = boto3.client("s3")
    bucket = file_path.split("/")[2]
    key = "/".join(file_path.split("/")[3:])

    try:
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        excel_data = s3_object["Body"].read()
        excel_file = pd.ExcelFile(BytesIO(excel_data))
    except Exception as e:
        print(f"Error reading Excel file from S3 path: {file_path}")
        raise e

    sheet_dfs = []
    for sheet_name in excel_file.sheet_names:
        pandas_df = pd.read_excel(excel_file, sheet_name=sheet_name)
        spark_df = spark_session.createDataFrame(pandas_df.astype(str))
        sheet_dfs.append(spark_df)

    if not sheet_dfs:
        return spark_session.createDataFrame(
            [], schema=...
        )  # Return empty DF if no sheets

    combined_df = sheet_dfs[0]
    for i in range(1, len(sheet_dfs)):
        combined_df = combined_df.union(sheet_dfs[i])

    return combined_df


# --- Main ETL Logic ---

# 1. Read source data using the helper function
source_df = read_excel_from_s3(spark, s3_input_path)
print(f"Read {source_df.count()} total records from all sheets.")

# 2. Deduplicate data based on the unique order identifier
deduplicated_df = source_df.dropDuplicates(["order_id"])
print(f"Found {deduplicated_df.count()} records after deduplication.")
