"""
PySpark Structured Streaming — reads from Kafka and prints a running word count.

Dependencies (install the JAR via the spark submit config below, or set PYSPARK_SUBMIT_ARGS):
    The Kafka connector JAR is pulled automatically via the `--packages` flag.

Run:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        consumer.py

Or set the env var and run with `python consumer.py`:
    export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"
    python consumer.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, window

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell",
)

KAFKA_BROKER = "localhost:9092"
TOPIC = "word-count-input"

# ---------------------------------------------------------------------------
# 1. Create Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("KafkaWordCount")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")  # keep it small for local dev
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Read the raw stream from Kafka
#    Each row has: key, value, topic, partition, offset, timestamp, ...
#    The message body lives in `value` as bytes.
# ---------------------------------------------------------------------------
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")  # only process new messages
    .load()
)

# ---------------------------------------------------------------------------
# 3. Parse the value bytes into a string sentence
# ---------------------------------------------------------------------------
sentences = raw_stream.select(
    col("value").cast("string").alias("sentence"),
    col("timestamp"),  # Kafka event timestamp — used for the time window
)

# ---------------------------------------------------------------------------
# 4. Split each sentence into individual words (explode = one row per word)
# ---------------------------------------------------------------------------
words = sentences.select(
    explode(split(col("sentence"), r"\s+")).alias("word"),
    col("timestamp"),
)

# ---------------------------------------------------------------------------
# 5. Count words inside a 30-second tumbling window, updating every 10 seconds
#    - window()  groups events that fall in the same time bucket
#    - watermark  tells Spark how late data can arrive before it is dropped
# ---------------------------------------------------------------------------
word_counts = (
    words.withWatermark("timestamp", "20 seconds")
    .groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),  # (window size, slide)
        col("word"),
    )
    .count()
    .orderBy("window", "count", ascending=[True, False])
)

# ---------------------------------------------------------------------------
# 6. Write to console in "complete" output mode so we always see the full table
# ---------------------------------------------------------------------------
query = (
    word_counts.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 50)
    .trigger(processingTime="10 seconds")  # micro-batch every 10 s
    .start()
)

print("Streaming query started. Waiting for data from Kafka…")
print(f"  broker : {KAFKA_BROKER}")
print(f"  topic  : {TOPIC}\n")

query.awaitTermination()
