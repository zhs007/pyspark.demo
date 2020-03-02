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

# df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
#                                       driver="com.mysql.jdbc.Driver",
#                                       dbtable="(SELECT uid FROM gamelog6_api_200227 WHERE curtime >= '2020-02-27') tmp",
#                                       user=cfg['gamelogdb']['user'],
#                                       password=cfg['gamelogdb']['password']).load().cache()

df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                      driver="com.mysql.jdbc.Driver",
                                      dbtable="(SELECT distinct(uid) FROM gamelog6_api_200227 WHERE curtime >= '2020-02-27') tmp",
                                      user=cfg['gamelogdb']['user'],
                                      password=cfg['gamelogdb']['password']).load().cache()                                      

# jdbcDf.write.saveAsTable(name='gamelog6_api_200227', mode='overwrite')
print("mysql", df1.printSchema())
print("mysql count", df1.count())

df2 = df1.distinct()

df2.write.jdbc(url=cfg['outputdb']['host'],
               mode="overwrite",
               table="uid_200227",
               properties={"driver": 'com.mysql.jdbc.Driver',
                           "user": cfg['outputdb']['user'],
                           "password": cfg['outputdb']['password']})

# df1.write.parquet("output/gamelog6_api_200227.parquet")

spark.stop()
