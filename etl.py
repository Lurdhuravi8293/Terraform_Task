# simple_transform.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create Spark session (Glue will handle this automatically if run in Glue)
spark = SparkSession.builder.appName("SimpleETL").getOrCreate()

# ====== SETTINGS ======
input_path = "s3://<YOUR_BUCKET>/data/students_social_media_addiction.csv"  # change <YOUR_BUCKET>
output_path = "s3://<YOUR_BUCKET>/output/"                                  # change <YOUR_BUCKET>

# ====== READ CSV ======
df = spark.read.option("header", True).csv(input_path)

# ====== SIMPLE TRANSFORMATIONS ======
# 1. Convert "Age" to integer
df = df.withColumn("Age", col("Age").cast("int"))

# 2. Create a new column to check if usage > 4 hours
df = df.withColumn("High_Usage", when(col("Avg_Daily_Usage_Hours").cast("double") > 4, "Yes").otherwise("No"))

# 3. Filter only rows where Age >= 18
df = df.filter(col("Age") >= 18)

# ====== WRITE RESULT BACK TO S3 ======
df.write.mode("overwrite").option("header", True).csv(output_path)

print(f"Data transformed and saved to {output_path}")
