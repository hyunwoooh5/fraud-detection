from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def create_spark_session(app_name="Fraud detection ETL"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    return spark


def define_schema():
    return StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True)
    ])


def transform_data(df):

    df = df.withColumn("errorBalanceOrig", F.col("newbalanceOrig") + F.col("amount") - F.col("oldbalanceOrg")) \
           .withColumn("errorBalanceDest", F.col("oldbalanceDest") + F.col("amount") - F.col("newbalanceDest"))

    # nameOrig: customer who started the transaction
    w = Window.partitionBy("nameOrig").orderBy("step")

    df = df.withColumn("stepDiff", F.col("step") - F.lag("step", 1).over(w))

    w_avg = Window.partitionBy("nameOrig").orderBy("step").rowsBetween(-2, 0)
    df = df.withColumn("avgAmtLast3", F.avg("amount").over(w_avg))

    df = df.withColumn("isCashOut", F.when(F.col("type") == "CASH_OUT", 1).otherwise(0)) \
        .withColumn("isCashIn", F.when(F.col("type") == "CASH_IN", 1).otherwise(0))

    return df


def main(input_path="data/raw_augmented", output_path="data/transactions_augmented_parquet", n_partitions=50):
    spark = create_spark_session()

    df = spark.read.csv(input_path, header=True, schema=define_schema())

    processed_df = transform_data(df)

    processed_df.select("step", "nameOrig", "amount",
                        "errorBalanceOrig", "stepDiff", "avgAmtLast3").show(5)

    # To reduce memory burden
    processed_df.repartition(n_partitions) \
        .write \
        .mode("overwrite") \
        .parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
