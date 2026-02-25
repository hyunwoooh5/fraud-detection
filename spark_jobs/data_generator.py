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


def main(replication_factor=10, seed=42):
    spark = create_spark_session()

    INPUT_PATH = "data/paysim.csv"
    OUTPUT_PATH = "data/raw_augmented"

    REPLICATION_FACTOR = replication_factor
    MAX_STEP_IN_ORIGINAL = 743  # ~31 days, 743/24 = 30.9583

    df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

    # add randomness
    replication_df = spark.range(REPLICATION_FACTOR) \
        .withColumnRenamed("id", "replicated_id") \
        .withColumn("random_offset", (F.rand(seed=seed) * MAX_STEP_IN_ORIGINAL).cast("int")) \
        .withColumn("reverse_direction", F.when(F.rand(seed=seed+1) > 0.5, True).otherwise(False))

    augmented_df = df.crossJoin(replication_df)

    # 1. Circular shift
    augmented_df = augmented_df \
        .withColumn("local_step", F.col("step") + F.col("random_offset") % MAX_STEP_IN_ORIGINAL)

    # 2. Time reversal
    augmented_df = augmented_df \
        .withColumn("local_step",
                    F.when(F.col("reverse_direction"),
                           F.lit(MAX_STEP_IN_ORIGINAL) - F.col("local_step"))
                    .otherwise(F.col("local_step"))
                    )

    # 3. Global timestamp
    augmented_df = augmented_df \
        .withColumn("step", F.col("local_step") + F.col("replicated_id") * MAX_STEP_IN_ORIGINAL)

    cols_to_drop = ["replicated_id", "random_offset",
                    "reverse_direction", "local_step"]
    augmented_df = augmented_df.drop(*cols_to_drop)

    augmented_df.repartition(replication_factor) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main(replication_factor=10)
