# -*- coding: UTF-8 -*-

import sys
import yaml
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
import socket


def getHostIP():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('spark-master', 8080))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip


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
    df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr1,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    sqlstr2 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        yesterday.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df2 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr2,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    sqlstr3 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime < '%s') tmp" % (
        tomorrow.strftime("%y%m%d"), tomorrow.strftime("%Y-%m-%d"))
    df3 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr3,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    # print("loadUsersInDay %s count is %d, %d, %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count(), df2.count(), df3.count()))

    curdaystr = daytime.strftime("%Y-%m-%d")
    df1 = df1.union(df2).union(df3).distinct().withColumn(
        'day', F.lit(curdaystr)).cache()
    # df1 = df1.union(df2)
    # df1 = df1.union(df3)
    # df1 = df1.distinct()
    # df1.cache()

    # print("loadUsersInDay %s total count is %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count()))

    # df1.write.parquet("output/usersinday_%s.parquet" %
    #                   daytime.strftime("%y%m%d"))

    return df1


def loadUsersInDay1(ctx, cfg, daytime):
    """获取这一天有操作的用户
    因为原始数据表是按天分表了，但由于服务器时间时间不能完全同步，导致少量数据会放错表
    所以需要读取前后一共3张表

    这种写法，实测 23分30秒
    """
    if not isinstance(daytime, (datetime)):
        raise TypeError('loadUsersInDay: daytime is not a datetime.')

    yesterday = daytime - timedelta(days=1)
    tomorrow = daytime + timedelta(days=1)

    sqlstr1 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        daytime.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr1,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    sqlstr2 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        yesterday.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df2 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr2,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    sqlstr3 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime < '%s') tmp" % (
        tomorrow.strftime("%y%m%d"), tomorrow.strftime("%Y-%m-%d"))
    df3 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr3,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    # print("loadUsersInDay %s count is %d, %d, %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count(), df2.count(), df3.count()))

    # df1 = df1.union(df2).union(df3).distinct().cache()
    df1 = df1.union(df2)
    df1 = df1.union(df3)
    df1 = df1.distinct()

    # print("loadUsersInDay %s total count is %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count()))

    # df1.write.parquet("output/usersinday_%s.parquet" %
    #                   daytime.strftime("%y%m%d"))

    return df1


def loadUsersInDay2(ctx, cfg, daytime):
    """获取这一天有操作的用户
    因为原始数据表是按天分表了，但由于服务器时间时间不能完全同步，导致少量数据会放错表
    所以需要读取前后一共3张表

    这种写法，实测耗时 3分
    """
    if not isinstance(daytime, (datetime)):
        raise TypeError('loadUsersInDay: daytime is not a datetime.')

    yesterday = daytime - timedelta(days=1)
    tomorrow = daytime + timedelta(days=1)

    sqlstr1 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        daytime.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr1,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    sqlstr2 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        yesterday.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df2 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr2,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    sqlstr3 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime < '%s') tmp" % (
        tomorrow.strftime("%y%m%d"), tomorrow.strftime("%Y-%m-%d"))
    df3 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr3,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    # print("loadUsersInDay %s count is %d, %d, %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count(), df2.count(), df3.count()))

    df1 = df1.union(df2).union(df3).distinct().cache()
    # df1 = df1.union(df2)
    # df1 = df1.union(df3)
    # df1 = df1.distinct()

    # print("loadUsersInDay %s total count is %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count()))

    # df1.write.parquet("output/usersinday_%s.parquet" %
    #                   daytime.strftime("%y%m%d"))

    return df1


def loadUsersInDay3(ctx, cfg, daytime):
    """获取这一天有操作的用户
    因为原始数据表是按天分表了，但由于服务器时间时间不能完全同步，导致少量数据会放错表
    所以需要读取前后一共3张表

    这种写法，实测 3分
    """
    if not isinstance(daytime, (datetime)):
        raise TypeError('loadUsersInDay: daytime is not a datetime.')

    yesterday = daytime - timedelta(days=1)
    tomorrow = daytime + timedelta(days=1)

    sqlstr1 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        daytime.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr1,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    sqlstr2 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        yesterday.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df2 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr2,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    sqlstr3 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime < '%s') tmp" % (
        tomorrow.strftime("%y%m%d"), tomorrow.strftime("%Y-%m-%d"))
    df3 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr3,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    # print("loadUsersInDay %s count is %d, %d, %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count(), df2.count(), df3.count()))

    # df1 = df1.union(df2).union(df3).distinct().cache()
    df1 = df1.union(df2)
    df1 = df1.union(df3)
    df1 = df1.distinct()
    df1.cache()

    # print("loadUsersInDay %s total count is %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count()))

    # df1.write.parquet("output/usersinday_%s.parquet" %
    #                   daytime.strftime("%y%m%d"))

    return df1


def loadUsersInDay4(ctx, cfg, daytime):
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
    df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr1,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load().cache()

    sqlstr2 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime >= '%s') tmp" % (
        yesterday.strftime("%y%m%d"), daytime.strftime("%Y-%m-%d"))
    df2 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr2,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load().cache()

    sqlstr3 = "(SELECT distinct(uid) as uid FROM gamelog6_api_%s WHERE curtime < '%s') tmp" % (
        tomorrow.strftime("%y%m%d"), tomorrow.strftime("%Y-%m-%d"))
    df3 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr3,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load().cache()

    # print("loadUsersInDay %s count is %d, %d, %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count(), df2.count(), df3.count()))

    df1e = df1.union(df2).union(df3).distinct().cache()
    df1.unpersist()
    df2.unpersist()
    df3.unpersist()
    # df1 = df1.union(df2)
    # df1 = df1.union(df3)
    # df1 = df1.distinct()
    # df1.cache()

    # print("loadUsersInDay %s total count is %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count()))

    # df1.write.parquet("output/usersinday_%s.parquet" %
    #                   daytime.strftime("%y%m%d"))

    return df1e


def countRetentionRate(dfdict, daytime, enddaytime):
    """统计留存率
    """
    if not isinstance(daytime, (datetime)):
        raise TypeError('countRetentionRate: daytime is not a datetime.')
    if not isinstance(enddaytime, (datetime)):
        raise TypeError('countRetentionRate: enddaytime is not a datetime.')

    curkey = daytime.strftime("%y%m%d")
    lstrr = [1.0]
    daytime = daytime + timedelta(days=1)
    curdf = dfdict.get(curkey)
    if curdf == None:
        return lstrr

    while daytime < enddaytime:
        cdtstr = daytime.strftime("%y%m%d")
        cdf = dfdict.get(cdtstr)
        if cdf == None:
            return lstrr

        cdf = curdf.subtract(cdf)
        lstrr.append((float(curdf.count()) - float(cdf.count())
                      ) / float(curdf.count()))

        daytime = daytime + timedelta(days=1)

    return lstrr


def loadAccount(ctx, cfg):
    sqlstr = "(select uid, DATE_FORMAT(regtime, '%Y-%m-%d') as regtime from account) tmp"
    df = ctx.read.format("jdbc").options(url=cfg['gamedb']['host'],
                                         driver="com.mysql.jdbc.Driver",
                                         dbtable=sqlstr,
                                         user=cfg['gamedb']['user'],
                                         password=cfg['gamedb']['password']).load().cache()
    return df


def countUserRegTime(accountdf, df):
    return df.join(accountdf,
                   df.uid == accountdf.uid,
                   'inner').select(F.datediff(df.day, accountdf.regtime).alias('daydiff')).groupBy('daydiff').count().sort(F.asc('daydiff'))


myip = getHostIP()

f = open('/app/config.yaml')
cfg = yaml.load(f)

spark = SparkSession.builder.appName("retention rate").config(
    "spark.driver.host", myip).getOrCreate()
ctx = SQLContext(spark.sparkContext)

accountdf = loadAccount(ctx, cfg)

startdt = datetime(2020, 1, 1)
enddt = datetime(2020, 1, 2)

dts = startdt
dayoff = 0
dfdict = {}
lstusers = []

# dfstart = loadUsersInDay(ctx, cfg, dts)
# dts = dts + timedelta(days=1)
# lstrr = [float(dfstart.count()) / float(dfstart.count())]
# lstusers = [dfstart.count()]

while dts < enddt:
    df = loadUsersInDay(ctx, cfg, dts)
    cdtstr = dts.strftime("%y%m%d")
    dfdict[cdtstr] = df
    lstusers.append(df.count())

    dts = dts + timedelta(days=1)
    dayoff = dayoff + 1

    # if dayoff > 30:
    #     break

dts = startdt
rrdict = {}
maxlen = 0

while dts < enddt:
    curlst = countRetentionRate(dfdict, dts, enddt)
    cdtstr = dts.strftime("%y%m%d")
    if maxlen < len(curlst):
        maxlen = len(curlst)

    if len(curlst) < maxlen:
        for i in range(maxlen - len(curlst)):
            curlst.append(np.nan)

    rrdict[cdtstr] = curlst

    dts = dts + timedelta(days=1)

rrdf = pd.DataFrame(rrdict)

print("retention rate is ", rrdf)
print("users is ", lstusers)

print(countUserRegTime(accountdf, dfdict[startdt.strftime("%y%m%d")]).show())

spark.stop()
