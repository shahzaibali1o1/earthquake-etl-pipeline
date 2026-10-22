from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def transform_bronze_to_silver(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()

    raw = spark.read.option("multiLine", True).json(input_path)
    exploded = raw.selectExpr("explode(features) as feature")

    silver = exploded.select(
        col("feature.id").alias("event_id"),
        col("feature.properties.mag").alias("magnitude"),
        col("feature.properties.place").alias("place"),
        (col("feature.properties.time") / 1000).cast("timestamp").alias("event_time"),
        (col("feature.properties.updated") / 1000).cast("timestamp").alias("updated_time"),
        col("feature.properties.tsunami").cast("integer").alias("tsunami"),
        col("feature.geometry.coordinates").getItem(0).cast("double").alias("longitude"),
        col("feature.geometry.coordinates").getItem(1).cast("double").alias("latitude"),
        col("feature.geometry.coordinates").getItem(2).cast("double").alias("depth_km"),
    )

    silver = silver.withColumn("event_date", to_date(col("event_time")))
    silver = silver.filter(col("magnitude").isNotNull())

    silver.write.mode("overwrite").partitionBy("event_date").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    transform_bronze_to_silver(
        input_path="data/bronze/raw_2026-01-01.json",
        output_path="data/silver/",
    )
