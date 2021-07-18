#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import *
from datetime import datetime, date


# # Start

# In[2]:


spark = SparkSession.builder.appName("HelloPySpark").getOrCreate()


# In[3]:


print("spark.version ==", spark.version)


# ### RDD

# In[4]:


rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])


# In[5]:


rdd.foreach(print)


# In[6]:


for r in rdd.take(2):
    print(r)


# ### DataFrame

# In[7]:


df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')


# In[8]:


df.show()


# In[9]:


df.printSchema()


# #### Selecting and Accessing

# In[10]:


df.withColumn('upper_c', upper(df.c)).show()


# In[11]:


df.select(df.c).show()


# In[12]:


df.filter(df.a == 1).show()


# #### Grouping Data

# In[13]:


df2 = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])


# In[14]:


df2.show()


# In[15]:


df2.groupby('color').avg().show()


# ## SQL

# In[16]:


df2.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show()


# # Stop

# In[17]:


spark.stop()

