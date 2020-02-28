import sys
import yaml
from pyspark import SparkContext
from pyspark.sql import SQLContext

f = open('config.yaml')
cfg = yaml.load(f)

sc = SparkContext("local", "mysql app")
ctx = SQLContext(sc)

jdbcDf = ctx.read.format("jdbc").options(url=cfg['mysql']['host'],
                                       driver="com.mysql.jdbc.Driver",
                                       dbtable="(SELECT * FROM gamelog6_api_200227 WHERE curtime >= '2020-02-27') tmp",
                                       user=cfg['mysql']['user'],
                                       password=cfg['mysql']['password']).load()
jdbcDf.write().saveAsTable('gamelog6_api_200227')                                       
print("mysql", jdbcDf.printSchema())
print("mysql count", jdbcDf.count())