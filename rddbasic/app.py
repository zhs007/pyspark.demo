# -*- coding: UTF-8 -*-

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd basic").getOrCreate()
words = spark.parallelize(
    ["scala",
     "java",
     "hadoop",
     "spark",
     "akka",
     "spark vs hadoop",
     "pyspark",
     "pyspark and spark"]
)
counts = words.count()
print("Number of elements in RDD -> %i" % (counts))

spark.stop()