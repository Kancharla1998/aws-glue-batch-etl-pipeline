import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ---------- 1. SETUP ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_PATH = f"s3://{args['BUCKET']}/raw/orders/"
OUT_PATH = f"s3://{args['BUCKET']}/processed/orders/"

# ---------- 2. EXTRACT: read only NEW files ----------
raw = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [RAW_PATH], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="read_raw_orders",
)
df = raw.toDF()
print(f"New rows found: {df.count()}")

# ---------- 3. TRANSFORM: clean the data ----------
if df.count() > 0:
    clean = (
        df.withColumn("quantity", F.col("quantity").cast("int"))
          .withColumn("unit_price", F.col("unit_price").cast("double"))
          .withColumn("total_amount", F.col("total_amount").cast("double"))
          .withColumn("order_ts", F.to_timestamp("order_date"))
          .withColumn("order_date", F.to_date("order_ts"))
          .withColumn("order_status", F.lower(F.trim("order_status")))
          .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
          .filter(F.col("quantity") > 0)
          .dropDuplicates(["order_id"])
          .withColumn("processed_at", F.current_timestamp())
    )
    print(f"Clean rows: {clean.count()}")

    # ---------- 4. LOAD: write Parquet, one folder per day ----------
    clean.write.mode("append").partitionBy("order_date").parquet(OUT_PATH)

# ---------- 5. COMMIT: remember which files are done ----------
job.commit()