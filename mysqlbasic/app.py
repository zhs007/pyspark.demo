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
ctx = SQLContext(sparkSession=spark)

df1 = ctx.read.format("jdbc").options(url=cfg['mysql']['host'],
                                      driver="com.mysql.jdbc.Driver",
                                      dbtable="(SELECT distinct(uid) as uid FROM gamelog6_api_200227 WHERE curtime >= '2020-02-27') tmp",
                                      user=cfg['mysql']['user'],
                                      password=cfg['mysql']['password']).load()

df2 = ctx.read.format("jdbc").options(url=cfg['mysql']['host'],
                                      driver="com.mysql.jdbc.Driver",
                                      dbtable="(SELECT distinct(uid) as uid FROM gamelog6_api_200226 WHERE curtime >= '2020-02-27') tmp",
                                      user=cfg['mysql']['user'],
                                      password=cfg['mysql']['password']).load()

df3 = ctx.read.format("jdbc").options(url=cfg['mysql']['host'],
                                      driver="com.mysql.jdbc.Driver",
                                      dbtable="(SELECT distinct(uid) as uid FROM gamelog6_api_200228 WHERE curtime < '2020-02-28') tmp",
                                      user=cfg['mysql']['user'],
                                      password=cfg['mysql']['password']).load()

# jdbcDf.write.saveAsTable(name='gamelog6_api_200227', mode='overwrite')
print("mysql", df1.printSchema())
print("mysql 1 count", df1.count())
print("mysql 2 count", df2.count())
print("mysql 3 count", df3.count())

df1 = df1.union(df2)
df1 = df1.union(df3)
df1 = df1.distinct()

# df1.write.parquet("output/gamelog6_api_200227.parquet")

spark.stop()
