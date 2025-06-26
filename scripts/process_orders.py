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
