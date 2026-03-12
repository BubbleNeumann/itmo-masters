from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg
import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-topic', default='raw_events')
    parser.add_argument('--output-table', default='event_aggregates')
    parser.add_argument('--window-size', type=int, default=5)
    parser.add_argument('--slide-interval', type=int, default=1)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder \
        .appName("StreamingETL") \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", args.input_topic) \
        .load()

    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
        .withWatermark("timestamp", "10 minutes")

    aggregated = parsed.groupBy(  # windowed agg
        window(col("timestamp"),
               f"{args.window_size} minutes",
               f"{args.slide_interval} minutes"),
        col("event_type")
    ).agg(
        count("*").alias("event_count"),
        avg("value").alias("avg_value")
    )

    query = aggregated.writeStream \
        .foreachBatch(lambda df, epoch: write_aggregates(df, args.output_table)) \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()


def write_aggregates(df, table):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", table) \
        .mode("append") \
        .save()