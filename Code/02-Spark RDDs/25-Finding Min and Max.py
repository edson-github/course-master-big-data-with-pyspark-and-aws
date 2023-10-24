# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Min and Max")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/movie_ratings.csv')
rdd.collect()

# COMMAND ----------

x = 3
y = 4
min(x, y)

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0],int(x.split(',')[1])))
rdd2.collect()

# COMMAND ----------

rdd2.reduceByKey(lambda x , y: min(x, y)).collect()

# COMMAND ----------

rdd2.reduceByKey(lambda x , y: max(x, y)).collect()
