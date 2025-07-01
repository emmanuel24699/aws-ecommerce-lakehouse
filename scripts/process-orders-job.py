import sys
import logging
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)
from delta.tables import DeltaTable


# --- Logger and S3 Upload Setup ---
def setup_logger():
    log_file_path = "/tmp/glue_etl_run.log"
    logger = logging.getLogger("ETL_Logger")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file_path)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger, log_file_path


def upload_log_to_s3(local_path, s3_bucket, job_name):
    try:
        s3_client = boto3.client("s3")
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        s3_key = f"logs/{job_name}/{timestamp}.log"
        s3_client.upload_file(local_path, s3_bucket, s3_key)
        print(f"Successfully uploaded log to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload log to S3: {e}")


# --- Main Script ---
logger, log_file_path = setup_logger()

try:
    # --- Initializations ---
    logger.info("Starting Orders ETL Job")
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "S3_INPUT_PATH", "S3_PROCESSED_ZONE", "S3_REJECTED_PATH"]
    )
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # --- Parameters ---
    s3_input_path = args["S3_INPUT_PATH"]
    s3_processed_zone = args["S3_PROCESSED_ZONE"]
    s3_rejected_path = args["S3_REJECTED_PATH"]
    orders_delta_path = f"{s3_processed_zone}orders/"
    logger.info(f"Input path: {s3_input_path}")

    # --- Define Explicit Schema ---
    orders_schema = StructType(
        [
            StructField("order_num", IntegerType(), True),
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField(
                "order_timestamp", StringType(), True
            ),  # Read as string, then parse
            StructField("total_amount", DoubleType(), True),
            StructField("date", DateType(), True),
        ]
    )

    # --- Helper Function to Read Excel ---
    def read_excel_from_s3(file_path, schema):
        logger.info(f"Reading Excel file from {file_path}")
        s3_client = boto3.client("s3")
        bucket = file_path.split("/")[2]
        key = "/".join(file_path.split("/")[3:])
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        excel_data = s3_object["Body"].read()
        excel_file = pd.ExcelFile(BytesIO(excel_data), engine="openpyxl")

        # Read all sheets into a single pandas DataFrame first
        all_sheets_df = pd.concat(
            [
                pd.read_excel(excel_file, sheet_name=sheet)
                for sheet in excel_file.sheet_names
            ],
            ignore_index=True,
        )

        # Create Spark DataFrame with the explicit schema
        return spark.createDataFrame(all_sheets_df, schema=schema)

    # --- Main ETL Logic ---
    source_df = read_excel_from_s3(s3_input_path, orders_schema)
    logger.info(f"Read {source_df.count()} total records from all sheets.")
    deduplicated_df = source_df.dropDuplicates(["order_id"])

    deduplicated_df.cache()
    valid_records_df = deduplicated_df.filter(col("order_id").isNotNull())
    rejected_records_df = deduplicated_df.filter(col("order_id").isNull())

    rejected_count = rejected_records_df.count()
    if rejected_count > 0:
        logger.warning(
            f"Found {rejected_count} rejected records. Writing to {s3_rejected_path}"
        )
        rejected_records_df.withColumn(
            "rejection_reason", lit("order_id is null")
        ).write.mode("append").format("json").save(s3_rejected_path)

    # Transform timestamp explicitly, other types are handled by the schema
    updates_df = valid_records_df.withColumn(
        "order_timestamp", to_timestamp(col("order_timestamp"))
    )

    valid_count = updates_df.count()
    logger.info(
        f"Merging {valid_count} valid records into Delta table at {orders_delta_path}"
    )

    if not DeltaTable.isDeltaTable(spark, orders_delta_path):
        logger.warning("Delta table not found. Creating a new one.")
        updates_df.write.format("delta").partitionBy("date").mode("overwrite").save(
            orders_delta_path
        )
    else:
        delta_table = DeltaTable.forPath(spark, orders_delta_path)
        delta_table.alias("target").merge(
            updates_df.alias("source"), "target.order_id = source.order_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    job.commit()
    logger.info("Orders ETL Job finished successfully.")

except Exception as e:
    logger.error(f"Job failed with error: {e}", exc_info=True)
    raise e

finally:
    logger.info("Attempting to upload log file to S3.")
    s3_bucket = "lab5-ecommerce-lakehouse"
    upload_log_to_s3(log_file_path, s3_bucket, args["JOB_NAME"])
