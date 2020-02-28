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
                                       dbtable="SELECT * FROM gamelog6_api_200228 limit 0, 1000",
                                       user=cfg['mysql']['user'],
                                       password=cfg['mysql']['password']).load()
print("mysql", jdbcDf.printSchema())