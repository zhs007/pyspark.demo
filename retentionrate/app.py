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
    """统计某一天的用户注册时长
    从全部注册用户里筛选出当天上线的用户，算出2个日期差，
    然后统计不同日期差里的用户数量并进行排序
    """
    return df.join(
        accountdf,
        df.uid == accountdf.uid,
        'inner').select(
            F.datediff(df.day, accountdf.regtime).alias('daydiff')).groupBy(
                'daydiff').count().sort(F.asc('daydiff'))


def loadUsersAndIPAddr(ctx, cfg, daytime):
    """获取这一天有操作的用户和他们的ip
    并根据同ip用户数量多少进行排序
    """
    if not isinstance(daytime, (datetime)):
        raise TypeError('loadUsersAndIPAddr: daytime is not a datetime.')

    sqlstr1 = "(SELECT distinct(uid) as uid, ipaddr FROM gamelog6_api_%s) tmp" % (
        daytime.strftime("%y%m%d"))
    df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr1,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    return df1.groupBy('ipaddr').count().sort(F.desc('count'))


def loadUserGames(ctx, cfg, daytime, mintimes):
    """获取这一天用户玩了哪些游戏并统计局数
    mintimes 是最小有效局数，我们会忽略不到这个局数的数据
    """
    if not isinstance(daytime, (datetime)):
        raise TypeError('loadUserGames: daytime is not a datetime.')

    sqlstr1 = "(SELECT uid, game, count(id) as count FROM gamelog6_api_%s GROUP BY uid, game) tmp" % (
        daytime.strftime("%y%m%d"))
    df1 = ctx.read.format("jdbc").options(url=cfg['gamelogdb']['host'],
                                          driver="com.mysql.jdbc.Driver",
                                          dbtable=sqlstr1,
                                          user=cfg['gamelogdb']['user'],
                                          password=cfg['gamelogdb']['password']).load()

    print("loadUserGames ", df1.printSchema())

    return df1.filter(df1.count > 100).cache()


def countGames(dfUserGames, dfUsers):
    """统计某一批用户里，不同游戏的游戏人数
    dfUserGames 从 loadUserGames 接口过来，已经忽略了游戏局数过少的数据
    在这里，单纯统计数据条数即可，也就是人数
    """

    return dfUserGames.join(
        dfUsers,
        dfUserGames.uid == dfUsers.uid,
        'inner').select(
            dfUserGames.uid,
            dfUserGames.game).groupBy(
                dfUserGames.game).count().sort(F.desc('count'))


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
print(loadUsersAndIPAddr(ctx, cfg, startdt).show())

dfUserGames = loadUserGames(ctx, cfg, startdt, 100)

tomorrow = startdt + timedelta(days=1)
df0 = dfdict[startdt.strftime("%y%m%d")]
df1 = dfdict[tomorrow.strftime("%y%m%d")]

df00 = df0.filter(df0.uid.isin(df1.uid))
df01 = df0.filter(~df0.uid.isin(df1.uid))

print(countGames(dfUserGames, df00).show())
print(countGames(dfUserGames, df01).show())

spark.stop()
