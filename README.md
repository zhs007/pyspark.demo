# pyspark.demo

这是我用来测试Spark的例子，0基础开始。  
不会花时间在接口和语法上，自己去官网查文档，那个是最靠谱的。  
希望能从这些例子上，你能对spark有足够的认识。

- saveAsTable 默认写在当前目录的 spark-warehouse 下。
- rdd实际和普通程序有差异，每次运算其实都会从头开始处理一遍，这里要活用cache。后面例子里会有不同实现的比较。

### 语言选型

选择的python，是因为都是接口调用，具体运算逻辑被封装到底层去了，语言层面效率差别其实不大。  
但如果你有非常多的复杂运算放在python层，其实还是会有影响的。  
因此，技术选型还是得看整体项目规划。

python和java、scale有一些差别，在部署上，最主要的是python不支持cluster模式，只能用默认的client方式。

### 运行环境搭建

我不太喜欢污染本地环境，所以提交了docker项目，建议使用docker，后面单机开多节点也方便些。  

之所以没有直接用``bde2020``的源，是因为我需要一些通用依赖，譬如mysql、numpy、pandas等，每个app都自己装依赖太麻烦了（numpy、pandas的安装非常非常慢......）。  
如果你的需求不一样，做法应该也会有少许差别。

注意，按 ``bde2020`` 官方脚本，实际上使用的是 ``python3`` （虽然系统默认的还是 ``python2.7`` ）。  
个人觉得这个方案比网上很多改系统默认python版本号要好。

下面是基本的使用步骤：

1. 进入docker\pythonapplocal目录，执行builddocker.sh文件。

```
sh builddocker.sh
```

2. 只启动master，使用startdocker.sh即可。

```
sh startdocker.sh
```

3. 进入容器bash。

4. 用下面的脚本来启动脚本，才能在webui里看到数据。

```
PYSPARK_PYTHON=python3 /spark/bin/spark-submit --master spark://spark-master:7077 main.py
```

5. 这时，应该可以通过webui来看到进展了。  
注意，我在启动脚本里将8080映射到了3722。

最后，如果不是经常改Dockerfile，就不需要重启docker，那样只需要启动脚本即可。

注意：

- python脚本里，如果要提到到master运行，创建的时候不能填local。

```python
# 这样是不行的
sc = SparkContext("local", "mysql app")

# 这样才是正确的
sc = SparkContext(appName="retention rate app")
```

- 也可以使用docker里的master、worker、pythonapp这样开启3个容器来使用。  
一个个的build、start即可。  
也可以用 ``docker-compose``。

（此处应该有张结构图）

- python依赖可以不用每个容器都装，submit的装好即可（因为python没有cluster模式）。

- 在docker下，如果有类似这样的报错 ``java.io.IOException: Failed to connect to b924c66ac091:34708``。  
本质上是因为worker需要和submit通信，但由于submit也是docker的，hostname默认给的是容器ID。  

解决方案很多，和部署关系很大。  
我提一个思路，submit节点其实是知道spark-master的，而和spark-master节点通信的ip地址，应该是可以被worker访问到的，所以通过spark-master获得ip地址，写入``spark.driver.host``即可。

```python
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

spark = SparkSession.builder.appName("rdd basic").config(
    "spark.driver.host", myip).getOrCreate()
```

- 如果executor报错，docker logs是查不到日志的，需要到 ``/spark/work/`` 里查日志。  
这个日志其实在webui上也能看到，但前提是那些端口要映射出来。

### 单机快速测试

下面是单机下的使用步骤：

1. 进入docker\pythonapplocal目录，执行builddocker.sh文件。

```
sh builddocker.sh
```

2. 参照``startdockerrr.sh``，执行具体的app即可。

### 熟悉环境和基本操作 -- rddbasic

这个项目很简单，最基本的rdd使用，把docker架设好，就可以运行python脚本。

主要是用来熟悉环境和基本操作的。

### 基本的SQL -- mysqlbasic

这里多了一个mysql操作，如果用我提供的docker，环境应该就是正常的，只需要配置一个mysql实例即可，建议用mysql5（我们线上环境还是mysql5......）。  
如果是mysql8的话，mysql connector需要换成8.x版。可以自行下载，然后放到docker目录下，重新build即可。  

spark的load其实并没有把数据全部读出来，就算这个例子实际的表是一张5m的表，如果我们仅仅只是count()，其实并不会很慢，大概50s左右（我们直接sql语句里count，只要0.01s），但如果我们要做一些复杂操作，譬如select某一列，再distinct，大概需要8分钟（而这种操作，放在sql语句里，只需要6s）。  

（此处应该有张表格）

因此，spark会做一定的优化，但没有那么智能，所以，能自己做的优化，还是得自己做。  

原则上，不需要读的数据不要读出来。

最后，关于写回mysql的一些问题：

- 如果目标表已建立，且有自增长字段，那么只要dataframe里没有这个字段，其实一切正常的。

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

在这个例子里，有4种不同的写法，结果如下：

``` python
    # 无cache，耗时23分30秒
    df1 = df1.union(df2)
    df1 = df1.union(df3)
    df1 = df1.distinct()

    # 结果cache，耗时3分
    df1 = df1.union(df2)
    df1 = df1.union(df3)
    df1 = df1.distinct()
    df1.cache()

    # 加cache且一行写完，耗时3分
    df1 = df1.union(df2).union(df3).distinct().cache()

    # 前面df1、df2、df3都在load以后cache，结束后unpersist
    # 这里属于过度cache，其实效率也没区别
    df1e = df1.union(df2).union(df3).distinct().cache()
    df1.unpersist()
    df2.unpersist()
    df3.unpersist()
```

这里之所以最后cache，是因为最后的结果后面还有用，而最初的3个dataframe其实后面就没用处了。  
结论是 cache 非常重要，而具体调用写法没影响。  
至于何时需要cache，第4种写法是个反例，建议自己多尝试体会一下。

因为spark是一个分布式运算框架，出于任务分派的考虑，实际上是在python层构建一个控制链，然后提交到不同的worker里去运行，如果没有cache，其实每个任务都是从头到尾顺序执行的，加了cache，可以由开发者来决策哪些步骤是可缓存的，整个实现方案会简单很多。

（此处应该有张结构图）