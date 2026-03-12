
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType


def parse_args():
    parser = argparse.ArgumentParser(description='Batch ETL from Kafka to ClickHouse')
    parser.add_argument('--input-topic', default='raw_events')
    parser.add_argument('--output-table', default='events')
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument('--dedup-key', default='event_id')
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder \
        .appName("BatchETL") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("value", IntegerType(), True),
        StructField("metadata", StringType(), True)
    ])
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", args.input_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    if args.dedup_key:
        parsed = parsed.dropDuplicates([args.dedup_key])

    parsed = parsed.withColumn("processed_at", current_timestamp())

    query = parsed.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_clickhouse(df, args.output_table)) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", f"/tmp/checkpoint_{args.input_topic}") \
        .start()

    query.awaitTermination()


def write_to_clickhouse(df, table):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", table) \
        .option("user", "default") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    main()