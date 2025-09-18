import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

schema = StructType() \
    .add("user_id", StringType()) \
    .add("session_id", StringType()) \
    .add("page_url", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType()) \
    .add("device", StringType()) \
    .add("country", StringType())

spark = (
    SparkSession.builder
    .appName("ClickstreamProcessor")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "clickstream-events")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

query = (
    parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
