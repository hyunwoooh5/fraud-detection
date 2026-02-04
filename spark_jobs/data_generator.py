import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(app_name="Data augmentation"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    return spark


def main(replication_factor=10):
    spark = create_spark_session()

    INPUT_PATH = "data/paysim.csv"
    OUTPUT_PATH = "data/raw_augmented"

    REPLICATION_FACTOR = replication_factor
    MAX_STEP_IN_ORIGINAL = 743

    df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

    replication_df = spark.range(
        REPLICATION_FACTOR).withColumnRenamed("id", "replicated_id")

    augmented_df = df.crossJoin(replication_df)

    augmented_df = augmented_df.withColumn("step", F.col(
        "step") + F.col("replicated_id") * MAX_STEP_IN_ORIGINAL)

    augmented_df = augmented_df.drop("replicated_id")

    augmented_df.repartition(replication_factor) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main(replication_factor=10)
