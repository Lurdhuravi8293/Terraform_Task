import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, row_number, rank
from pyspark.sql.window import Window

# --------------------------
# DEFAULT S3 PATHS
# --------------------------
SOURCE_S3_PATH = "s3://gitops-etl-raw-bucket-123456789/input/Students Social Media Addiction.csv"
OUTPUT_S3_PATH = "s3://gitops-etl-raw-bucket-123456789/output/"

# Glue job args
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Glue & Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------
# 1. Read CSV from S3
# --------------------------
df = spark.read.option("header", True).csv(SOURCE_S3_PATH)

# --------------------------
# 2. Convert data types
# --------------------------
df = df.withColumn("Avg_Daily_Usage_Hours", col("Avg_Daily_Usage_Hours").cast("double")) \
       .withColumn("Age", col("Age").cast("int"))

# --------------------------
# 3. Filter data
# --------------------------
df_filtered = df.filter(
    (col("Academic_Level") == "Undergraduate") &
    (col("Country") == "India") &
    (col("Avg_Daily_Usage_Hours") > 5)
)

# --------------------------
# 4. Add Usage_Category
# --------------------------
df_final = df_filtered.withColumn(
    "Usage_Category",
    when(col("Avg_Daily_Usage_Hours") >= 6, "High Usage")
    .when(col("Avg_Daily_Usage_Hours") >= 4, "Medium Usage")
    .otherwise("Low Usage")
)

# --------------------------
# 5. Window Transformations
# --------------------------

# Define a window for ranking by Avg_Daily_Usage_Hours per Country
window_spec = Window.partitionBy("Country").orderBy(col("Avg_Daily_Usage_Hours").desc())

df_windowed = df_final.withColumn("row_num", row_number().over(window_spec)) \
                      .withColumn("rank", rank().over(window_spec))

# --------------------------
# 6. Narrow Transformation (map equivalent)
# --------------------------
# In PySpark DataFrames, `.map()` is equivalent to `.rdd.map()` — here we create a short text description per row
df_mapped = df_windowed.rdd.map(lambda row: (
    row["Name"],  # keep student's name
    f"{row['Name']} from {row['Country']} has {row['Avg_Daily_Usage_Hours']} hrs/day usage - {row['Usage_Category']}"
))

# Convert back to DataFrame with proper column names
df_result = df_mapped.toDF(["Name", "Description"])

# --------------------------
# 7. Write back to S3 in Parquet
# --------------------------
df_result.write.mode("overwrite").parquet(OUTPUT_S3_PATH)

job.commit()
