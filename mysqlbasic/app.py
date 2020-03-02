# -*- coding: UTF-8 -*-

import sys
import yaml
from pyspark.sql import SparkSession, SQLContext
import socket


def getHostIP():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('spark-master', 8080))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip


myip = getHostIP()

f = open('/app/config.yaml')
cfg = yaml.load(f)

spark = SparkSession.builder.appName("mysql basic").config(
    "spark.driver.host", myip).getOrCreate()
ctx = SQLContext(spark.sparkContext)

df1 = ctx.read.format("jdbc").options(url=cfg['mysql']['host'],
                                      driver="com.mysql.jdbc.Driver",
                                      dbtable="(SELECT * FROM gamelog6_api_200227 WHERE curtime >= '2020-02-27') tmp",
                                      user=cfg['mysql']['user'],
                                      password=cfg['mysql']['password']).load()

# jdbcDf.write.saveAsTable(name='gamelog6_api_200227', mode='overwrite')
print("mysql", df1.printSchema())
print("mysql count", df1.count())

# df1.write.parquet("output/gamelog6_api_200227.parquet")

spark.stop()
