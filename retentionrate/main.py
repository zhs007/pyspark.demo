# -*- coding: UTF-8 -*-

import sys
import yaml
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
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

    # print("loadUsersInDay %s count is %d, %d, %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count(), df2.count(), df3.count()))

    df1 = df1.union(df2).union(df3).distinct().cache()
    # df1 = df1.union(df3)
    # df1 = df1.distinct()

    # print("loadUsersInDay %s total count is %d" %
    #       (daytime.strftime("%Y-%m-%d"), df1.count()))

    # df1.write.parquet("output/usersinday_%s.parquet" %
    #                   daytime.strftime("%y%m%d"))

    return df1


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


f = open('config.yaml')
cfg = yaml.load(f)

sc = SparkContext("local", "retention rate app")
ctx = SQLContext(sc)

startdt = datetime(2020, 1, 1)
enddt = datetime(2020, 1, 10)

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
