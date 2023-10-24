# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')
rdd.collect()

# COMMAND ----------

def foo(x):
  return x != '12 12 33'

rdd2 = rdd.filter(foo)
rdd2.collect()
