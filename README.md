# pyspark.demo

这是我用来测试Spark的例子，0基础开始。  

- 默认使用系统自带的python，一般都还是python 2.7。
- 很多操作，其实是未完成就返回的，譬如 load 等，这时，后续如果不是一些要求数据全部加载完的操作时，其实也是不需要等load完成的，会卡在那些要求加载完成的操作上。
- spark读mysql，5m条，大概40多s，但这个量级，写入非常慢（saveAsTable 或 parquet），而且会报错。所以设计上，不要缓存大数据。
- saveAsTable 默认写在当前目录的 spark-warehouse 下。
- spark底层的任务分派策略其实并没有那么智能，譬如我有个30张表的数据读取，还是会线性执行。我猜测如果批量的load，效率上会好些。当然，现在还没有开多个worker。后续再来研究这块优化细节。

### 关于语言选型

选择的python，是因为都是接口调用，具体运算逻辑被封装到底层去了，语言层面效率差别其实不大。  
但如果你有非常多的复杂运算放在python层，其实还是会有影响的。

### 运行环境搭建

我不太喜欢污染本地环境，所以提交了docker项目，建议使用docker，后面单机开多节点也方便些。  

之所以没有用bde的库，是因为我需要mysql支持，只能自己简单扩展一下，如果没有mysql需求，可以忽略。

### 熟悉环境和基本操作 -- rddbasic

这个项目很简单，最基本的rdd使用，把docker架设好，就可以运行python脚本。

主要是用来熟悉环境和基本操作的。

### 基本的SQL -- mysqlbasic

这里多了一个mysql操作，如果用我提供的docker，环境应该就是正常的，只需要配置一个mysql实例即可，建议用mysql5（我们线上环境还是mysql5......）。  
如果是mysql8的话，mysql connector需要换成8.x版。可以自行下载，然后放到docker目录下，重新build即可。

### 用户留存统计 -- retentionrate

统计用户留存率。  
每天一张mysql表，但由于服务器之间时间没有绝对同步，所以跨天时，少量数据会存到错误的表里去，这时需要读取3张表才能确定最终的数据。

``` python
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
```