import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

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
]  # "s3://lab5-ecommerce-lakehouse/raw/products.csv"
s3_processed_zone = args[
    "S3_PROCESSED_ZONE"
]  # "s3://lab5-ecommerce-lakehouse/processed/"
s3_rejected_path = args[
    "S3_REJECTED_PATH"
]  # "s3://lab5-ecommerce-lakehouse/rejected/products/"

products_delta_path = f"{s3_processed_zone}products/"


# 1. Read the raw CSV data from S3
try:
    source_df = spark.read.format("csv").option("header", "true").load(s3_input_path)
except Exception as e:
    print(f"Error reading source file from {s3_input_path}. Job failed.")
    raise e

# 2. Validation: Ensure primary identifier is not null
source_df.cache()
valid_records_df = source_df.filter(
    col("product_id").isNotNull() & (col("product_id") != "")
)
rejected_records_df = source_df.filter(
    col("product_id").isNull() | (col("product_id") == "")
)

# 3. Log rejected records if any exist
if rejected_records_df.count() > 0:
    print(
        f"Found {rejected_records_df.count()} rejected records. Writing to {s3_rejected_path}"
    )
    rejected_records_df.withColumn(
        "rejection_reason", lit("product_id is null")
    ).write.mode("append").format("json").save(s3_rejected_path)

# 4. Transform the data
updates_df = valid_records_df.select(
    col("product_id").cast(StringType()),
    col("department_id").cast(StringType()),
    col("department").cast(StringType()),
    col("product_name").cast(StringType()),
).dropDuplicates(["product_id"])
