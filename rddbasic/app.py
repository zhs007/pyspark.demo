# -*- coding: UTF-8 -*-

from pyspark.sql import SparkSession
import socket


def getHostIP():
    """
    查询本机ip地址
    :return: ip
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('spark-master', 8080))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip


myip = getHostIP()

spark = SparkSession.builder.appName("rdd basic").config(
    "spark.driver.host", myip).getOrCreate()

words = spark.sparkContext.parallelize(
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
