# -*- coding: UTF-8 -*-

import sys
import yaml
from datetime import datetime, timedelta
from pyspark import SparkContext
from pyspark.sql import SQLContext


def loadUsersInDay(ctx, cfg, daytime):
    """获取这一天有操作的用户
    因为原始数据表是按天分表了，但由于服务器时间时间不能完全同步，导致少量数据会放错表
    所以需要读取前后一共3张表
    """
    if not isinstance(daytime, (datetime)):
        raise TypeError('loadUsersInDay: daytime is not a datetime.')

    yesterday = daytime - timedelta(days=1)
    tomorrow = daytime + timedelta(days=1)

    sqlstr1 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        daytime.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df1 = ctx.read.format("jdbc").options(url=cfg['mysql']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr1,
                                          user=cfg['mysql']['user'],
                                          password=cfg['mysql']['password']).load()

    sqlstr2 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        yesterday.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df2 = ctx.read.format("jdbc").options(url=cfg['mysql']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr2,
                                          user=cfg['mysql']['user'],
                                          password=cfg['mysql']['password']).load()

    sqlstr3 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime < '%s') tmp" % (
        tomorrow.strftime("%y%m%d"), tomorrow.strftime("%Y-%m-%d"))
    df3 = ctx.read.format("jdbc").options(url=cfg['mysql']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr3,
                                          user=cfg['mysql']['user'],
                                          password=cfg['mysql']['password']).load()

    print("loadUsersInDay %s count is %d, %d, %d" %
          (daytime.strftime("%Y-%m-%d"), df1.count(), df2.count(), df3.count()))

    df1 = df1.union(df2)
    df1 = df1.union(df3)
    df1 = df1.distinct()

    print("loadUsersInDay %s total count is %d" %
          (daytime.strftime("%Y-%m-%d"), df1.count()))

    # df1.write.parquet("output/usersinday_%s.parquet" %
    #                   daytime.strftime("%y%m%d"))

    return df1


f = open('config.yaml')
cfg = yaml.load(f)

sc = SparkContext("local", "retention rate app")
ctx = SQLContext(sc)


dts = datetime(2020, 1, 1)
dte = datetime(2020, 2, 28)
dayoff = 0

dfstart = loadUsersInDay(ctx, cfg, dts)
dts = dts + timedelta(days=1)
lstrr = [float(dfstart.count()) / float(dfstart.count())]
lstusers = [dfstart.count()]

while dts < dte:
    df = loadUsersInDay(ctx, cfg, dts)
    cdf = dfstart.subtract(df)
    lstrr.append(float(cdf.count()) / float(dfstart.count()))
    lstusers.append(cdf.count())

    dts = dts + timedelta(days=1)
    dayoff = dayoff + 1

    if dayoff > 30:
        break

print("retention rate is ", lstrr)
print("users is ", lstusers)
