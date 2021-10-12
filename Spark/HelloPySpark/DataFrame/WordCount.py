from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("PythonWordCount") \
        .getOrCreate()

    lines = spark.read.text(sys.argv[1])
    counts = lines.withColumn("words", explode(split(col("value"), " "))).groupBy(col("words")).count()
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()
