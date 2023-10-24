# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("QUIZ")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Quiz_Sample.txt')
rdd.collect()

# COMMAND ----------

def quiz(x):
  l = x.split(' ')
  return [len(s) for s in l]
  
rdd2 = rdd.map(quiz)
rdd2.collect()

# COMMAND ----------

rdd3 = rdd.map(lambda x: [len(s) for s in x.split(' ')])
rdd3.collect()

# COMMAND ----------


