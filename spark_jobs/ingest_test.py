from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan

def main():
    spark = SparkSession.builder \
        .appName("FraudDetection local test") \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()
    
    file_path = "data/paysim.csv"

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    df.printSchema()

    row_count = df.count()
    print(f"Number of rows: {row_count}")

    df.groupBy("isFraud").agg(count("*").alias("count")).show()

    # df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show() # | = or

    dtypes_dict = dict(df.dtypes)
    
    check_exprs = []
    
    for c in df.columns:
        # isnan for double and float
        if dtypes_dict[c] in ['double', 'float']:
            check_exprs.append(count(when(isnan(c) | col(c).isNull(), c)).alias(c))
        # NULL for string, int, tiestamp...
        else:
            check_exprs.append(count(when(col(c).isNull(), c)).alias(c))

    df.select(check_exprs).show()

    spark.stop()

if __name__ == "__main__":
    main()
