#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, date

# Start
spark = SparkSession.builder.appName("HelloPySpark").getOrCreate()
print("spark.version ==", spark.version)

# RDD
rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])

rdd.foreach(print)

for r in rdd.take(2):
    print(r)

# DataFrame
df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')

df.show()
df.printSchema()

# Selecting and Accessing
df.withColumn('upper_c', upper(df.c)).show()
df.select(df.c).show()
df.filter(df.a == 1).show()

# Grouping Data
df2 = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])

df2.show()
df2.groupby('color').avg().show()

# SQL
df2.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show()

# Stop
spark.stop()
