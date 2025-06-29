import sys
import logging
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
from delta.tables import DeltaTable


# --- Logger and S3 Upload Setup ---
def setup_logger():
    """Sets up a logger to write to a local file."""
    log_file_path = "/tmp/glue_etl_run.log"
    logger = logging.getLogger("ETL_Logger")
    logger.setLevel(logging.INFO)

    # Create file handler
    handler = logging.FileHandler(log_file_path)
    handler.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    # Add the handler to the logger
    if not logger.handlers:
        logger.addHandler(handler)

    return logger, log_file_path


def upload_log_to_s3(local_path, s3_bucket, job_name):
    """Uploads the local log file to a specified S3 path."""
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
    logger.info("Starting Product ETL Job")
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
    products_delta_path = f"{s3_processed_zone}products/"
    logger.info(f"Input path: {s3_input_path}")

    # --- Main ETL Logic ---
    logger.info("Reading source CSV data from S3")
    source_df = spark.read.format("csv").option("header", "true").load(s3_input_path)

    source_df.cache()
    valid_records_df = source_df.filter(
        col("product_id").isNotNull() & (col("product_id") != "")
    )
    rejected_records_df = source_df.filter(
        col("product_id").isNull() | (col("product_id") == "")
    )

    rejected_count = rejected_records_df.count()
    if rejected_count > 0:
        logger.warning(
            f"Found {rejected_count} rejected records. Writing to {s3_rejected_path}"
        )
        rejected_records_df.withColumn(
            "rejection_reason", lit("product_id is null")
        ).write.mode("append").format("json").save(s3_rejected_path)

    updates_df = valid_records_df.select(
        col("product_id").cast(StringType()),
        col("department_id").cast(StringType()),
        col("department").cast(StringType()),
        col("product_name").cast(StringType()),
    ).dropDuplicates(["product_id"])

    valid_count = updates_df.count()
    logger.info(
        f"Merging {valid_count} valid records into Delta table at {products_delta_path}"
    )

    if not DeltaTable.isDeltaTable(spark, products_delta_path):
        logger.warning("Delta table not found. Creating a new one.")
        updates_df.write.format("delta").mode("overwrite").save(products_delta_path)
    else:
        delta_table = DeltaTable.forPath(spark, products_delta_path)
        delta_table.alias("target").merge(
            updates_df.alias("source"), "target.product_id = source.product_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    job.commit()
    logger.info("Product ETL Job finished successfully.")

except Exception as e:
    logger.error(f"Job failed with error: {e}")
    raise e

finally:
    # --- Upload Log File ---
    logger.info("Attempting to upload log file to S3.")
    s3_bucket = "lab5-ecommerce-lakehouse"
    upload_log_to_s3(log_file_path, s3_bucket, args["JOB_NAME"])
    logger.info("Log file uploaded successfully.")
