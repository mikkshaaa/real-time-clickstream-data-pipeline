from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_timestamp, min as Fmin, max as Fmax, count as Fcount
from delta.tables import DeltaTable
import os

# -----------------------------
# Spark Session with Delta Lake 4.0.0
# -----------------------------
spark = SparkSession.builder \
    .appName("ClickstreamBatchTransform") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Paths
# -----------------------------
bronze_path = "./data_lake/bronze/clickstream"
silver_path = "./data_lake/silver/session_events"
gold_path = "./data_lake/gold/session_summary"

# -----------------------------
# Bootstrap Bronze if missing
# -----------------------------
if not os.path.exists(bronze_path) or not DeltaTable.isDeltaTable(spark, bronze_path):
    print("⚡ Bronze table not found. Creating sample clickstream data...")

    sample_data = [
        Row(user_id="u1", session_id="s1", timestamp="2025-09-15T10:00:00Z"),
        Row(user_id="u1", session_id="s1", timestamp="2025-09-15T10:05:00Z"),
        Row(user_id="u2", session_id="s2", timestamp="2025-09-15T11:00:00Z"),
        Row(user_id="u2", session_id="s2", timestamp="2025-09-15T11:10:00Z"),
        Row(user_id="u3", session_id="s3", timestamp="2025-09-15T12:00:00Z"),
    ]
    df_bronze = spark.createDataFrame(sample_data)
    df_bronze.write.format("delta").mode("overwrite").save(bronze_path)

# -----------------------------
# Bronze → Silver
# -----------------------------
df = spark.read.format("delta").load(bronze_path)
df = df.withColumn("ts", to_timestamp("timestamp"))

if not os.path.exists(silver_path) or not DeltaTable.isDeltaTable(spark, silver_path):
    print("⚡ Silver table not found. Creating from Bronze...")
    df.write.format("delta").mode("overwrite").save(silver_path)
else:
    print("✅ Silver table already exists. Overwriting with fresh transform...")
    df.write.format("delta").mode("overwrite").save(silver_path)

# -----------------------------
# Silver → Gold (aggregate)
# -----------------------------
session_summary = (
    df.groupBy("user_id", "session_id")
      .agg(
          Fmin("ts").alias("session_start"),
          Fmax("ts").alias("session_end"),
          Fcount("*").alias("events_count")
      )
)

if not os.path.exists(gold_path) or not DeltaTable.isDeltaTable(spark, gold_path):
    print("⚡ Gold table not found. Creating from Silver...")
    session_summary.write.format("delta").mode("overwrite").save(gold_path)
else:
    print("✅ Gold table found. Merging new aggregates...")
    gold = DeltaTable.forPath(spark, gold_path)
    (gold.alias("t")
         .merge(session_summary.alias("s"), "t.session_id = s.session_id AND t.user_id = s.user_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

print("🎉 Batch transform completed successfully!")

# Stop Spark
spark.stop()
