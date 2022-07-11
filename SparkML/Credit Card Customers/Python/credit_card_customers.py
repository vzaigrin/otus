from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import *
import sys

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: CreditCardCustomers <path-to-model> <path-to-input> <path-to-output>")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .master("local[*]")\
        .appName("CreditCardCustomers") \
        .getOrCreate()

    model = PipelineModel.load(sys.argv[1])

    data = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[2])

    predicted = model.transform(data)

    predicted \
        .filter(col("prediction") == 1) \
        .select("CLIENTNUM") \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .csv(sys.argv[3])

    spark.stop()
