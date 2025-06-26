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

s3_input_path = args[
    "S3_INPUT_PATH"
]  # "s3://lab5-ecommerce-lakehouse/raw/orders-apr_2025.xlsx"
s3_processed_zone = args[
    "S3_PROCESSED_ZONE"
]  # "s3://lab5-ecommerce-lakehouse/processed/"
s3_rejected_path = args[
    "S3_REJECTED_PATH"
]  # "s3://lab5-ecommerce-lakehouse/rejected/orders/"

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
        # Convert pandas DataFrame to Spark DataFrame, ensuring schema consistency
        spark_df = spark_session.createDataFrame(pandas_df.astype(str))
        sheet_dfs.append(spark_df)

    # Combine all sheets into one DataFrame
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

# 3. Validation: Ensure primary identifier is not null
deduplicated_df.cache()
valid_records_df = deduplicated_df.filter(
    col("order_id").isNotNull() & (col("order_id") != "")
)
rejected_records_df = deduplicated_df.filter(
    col("order_id").isNull() | (col("order_id") == "")
)

# 4. Log rejected records
if rejected_records_df.count() > 0:
    print(
        f"Found {rejected_records_df.count()} rejected records. Writing to {s3_rejected_path}"
    )
    rejected_records_df.withColumn(
        "rejection_reason", lit("order_id is null")
    ).write.mode("append").format("json").save(s3_rejected_path)

# 5. Transform and cleanse data
# Convert timestamps and ensure correct data types
updates_df = valid_records_df.select(
    col("order_num").cast("int"),
    col("order_id").cast("string"),
    col("user_id").cast("string"),
    to_timestamp(col("order_timestamp")).alias("order_timestamp"),
    col("total_amount").cast("double"),
    col("date").cast("date"),
)

# 6. Write data to Delta table using MERGE (Upsert) logic
# This handles both new records and updates to existing ones.
print(
    f"Merging {updates_df.count()} valid records into Delta table at {orders_delta_path}"
)

try:
    delta_table = DeltaTable.forPath(spark, orders_delta_path)
    delta_table.alias("target").merge(
        updates_df.alias("source"), "target.order_id = source.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Merge operation successful.")
except Exception as e:
    # If the Delta table doesn't exist yet, create it
    if "is not a Delta table" in str(e):
        print("Delta table not found. Creating a new one.")
        updates_df.write.format("delta").partitionBy("date").mode("overwrite").save(
            orders_delta_path
        )
    else:
        raise e

job.commit()
print("Orders processing job finished successfully.")
