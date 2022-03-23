
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql import functions as F
import time
if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.\
        builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()

    """
    spark.sql.shuffle.partitions 参数是指，在sql计算中，shuffle算子截断默认的分区数是200个
    对于集群模式来说，200个也算合理
    如果在local下运行，200个很多，在调度上会带来额外的损耗
    所以在local下建议修改比较低  比如 2/4/6
    """


    sc=spark.sparkContext

    schema = StructType().\
        add("user_id",StringType(),nullable=True).\
        add("movie_id",StringType(),nullable=True).\
        add("rank",IntegerType(),nullable=True).\
        add("ts",StringType(),nullable=True)
    ##创建dataFrame
    df = spark.read.format("csv").\
        option("sep","\t").\
        option("header",False).\
        schema(schema).\
        load("../data/input/u.data")

   #TODO 1 ：用户平均分
    df.groupBy("user_id").\
        avg("rank").\
        withColumnRenamed("avg(rank)","avg_rank").\
        withColumn("avg_rank",F.round("avg_rank",2)).\
        orderBy("avg_rank",ascending=False).\
        show()
                # +-------+--------+
                # |user_id|avg_rank|
                # +-------+--------+
                # |    849|    4.87|
                # |    688|    4.83|
                # |    507|    4.72|
                # |    628|     4.7|
                # |    928|    4.69|
                # |    118|    4.66|
                # |    907|    4.57|
                # |    686|    4.56|
                # |    427|    4.55|
                # |    565|    4.54|
                # |    850|    4.53|
                # |    469|    4.53|
                # |    225|    4.52|
                # |    330|     4.5|
                # |    477|    4.46|
                # |    636|    4.45|
                # |    242|    4.45|
                # |    583|    4.44|
                # |    767|    4.43|
                # |    252|    4.43|
                # +-------+--------+


    #TODO 2: 电影的平均分

    df.createOrReplaceTempView("movie")
    spark.sql("select movie_id ,round(avg(rank),2) as avg_rank from movie group by movie_id").show()

    # +--------+--------+
    # | movie_id | avg_rank |
    # +--------+--------+
    # | 829 | 2.65 |
    # | 1436 | 2.5 |
    # | 467 | 3.79 |
    # | 691 | 3.5 |
    # | 1090 | 2.41 |
    # | 675 | 3.56 |
    # | 296 | 3.33 |
    # | 1512 | 4.0 |
    # | 1159 | 3.55 |
    # | 1572 | 1.0 |
    # | 451 | 3.35 |
    # | 944 | 3.0 |
    # | 125 | 3.56 |
    # | 853 | 3.86 |
    # | 1394 | 2.43 |
    # | 1372 | 3.0 |
    # | 800 | 2.88 |
    # | 1669 | 2.0 |
    # | 919 | 3.79 |
    # | 870 | 2.78 |
    # +--------+--------+

    #TODO 3: 大于平均分的电影数量

    # df.select(F.avg(df['rank'])).first()：算出的平均值很多行，取第一行
    # df.select(F.avg(df['rank'])).first()['avg(rank)']：取第一行的avg(rank)字段值
    print("大于平均分的电影数量:",df.where(df['rank'] > df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

    # +--------+--------+
    # | movie_id | avg_rank |
    # +--------+--------+
    # | 829 | 2.65 |
    # | 1436 | 2.5 |
    # | 467 | 3.79 |
    # | 691 | 3.5 |
    # | 1090 | 2.41 |
    # | 675 | 3.56 |
    # | 296 | 3.33 |
    # | 1512 | 4.0 |
    # | 1159 | 3.55 |
    # | 1572 | 1.0 |
    # | 451 | 3.35 |
    # | 944 | 3.0 |
    # | 125 | 3.56 |
    # | 853 | 3.86 |
    # | 1394 | 2.43 |
    # | 1372 | 3.0 |
    # | 800 | 2.88 |
    # | 1669 | 2.0 |
    # | 919 | 3.79 |
    # | 870 | 2.78 |
    # +--------+--------+

    #TODO 4:查询高分电影中（>3）打分次数最多的用户，此用户打分的平均分

    #先找到这个用户
    user_id = df.where("rank > 3").\
        groupBy("user_id").\
        count().\
        withColumnRenamed("count","cnt").\
        orderBy("cnt",ascending=False).\
        limit(1).\
        first()["user_id"]

    df.filter(df["user_id"]==user_id).\
        select(F.round(F.avg("rank"),2).alias("avg_rank")).show()

    # +-------------------+
    # | round(avg(rank), 0) |
    # +-------------------+
    # | 3.0 |
    # +-------------------+


    spark.sql("""  
      with t1 as (
      select 
      user_id,
      count (1) as cnt
      from movie
      where rank >3
      group by 
      user_id
      order by 
      cnt desc   
      limit 1 
      )select 
      t2.user_id,
      avg (t2.rank) as rank_avg
      from movie t2 inner join t1 on t1.user_id= t2.user_id
      group by 
      t2.user_id
    """ ).show()

    # +-------+------------------+
    # | user_id | rank_avg |
    # +-------+------------------+
    # | 450 | 3.8648148148148147 |
    # +-------+------------------+

    # TODO 5: 查询每个用户的平均打分，最高打分，最低打分
    # 1.sql方式
    spark.sql("""
    select 
    user_id,
    avg (rank) as avg_rank,
    max(rank) as max_rank,
    min(rank) as min_rank
    from movie
    group by 
    user_id
    """).show(5)

    # +-------+------------------+--------+--------+
    # | user_id | avg_rank | max_rank | min_rank |
    # +-------+------------------+--------+--------+
    # | 296 | 4.1768707482993195 | 5 | 1 |
    # | 467 | 3.6818181818181817 | 5 | 2 |
    # | 691 | 4.21875 | 5 | 1 |
    # | 675 | 3.7058823529411766 | 5 | 1 |
    # | 829 | 3.546875 | 5 | 1 |
    # +-------+------------------+--------+--------+

    # 2.DSL方式

    df.groupBy("user_id").agg(
        F.round(F.avg("rank"),2).alias("avg_rank"),
        F.round(F.max("rank"),2).alias("max_rank"),
        F.round(F.min("rank"),2).alias("min_rank")
    ).show(5)

    # +-------+--------+--------+--------+
    # | user_id | avg_rank | max_rank | min_rank |
    # +-------+--------+--------+--------+
    # | 296 | 4.0 | 5 | 1 |
    # | 467 | 4.0 | 5 | 2 |
    # | 691 | 4.0 | 5 | 1 |
    # | 675 | 4.0 | 5 | 1 |
    # | 829 | 4.0 | 5 | 1 |
    # +-------+--------+--------+--------+



    #TODO 6:查询评分超过100次的电影的平均分，取排名TOP10
    #1.sql
    spark.sql("""
    select 
    movie_id,
    round(avg(rank),2) as avg_rank
    from movie 
    group by 
    movie_id
    having count(movie_id) >100
    order by 
    avg_rank desc 
    limit 10
    """).show()


    # 2. DSL
    df.groupBy("movie_id").\
       agg(
        F.round(F.avg("rank"),2).alias("avg_rank"),
        F.round(F.count("movie_id"),2).alias("cnt")
    ).where("cnt>100").\
        sort("cnt",ascending=False).\
        limit(10).\
        select("movie_id","avg_rank").\
        show()

    # time.sleep(10000)

    # | movie_id | avg_rank |
    # +--------+--------+
    # | 50 | 4.36 |
    # | 258 | 3.8 |
    # | 100 | 4.16 |
    # | 181 | 4.01 |
    # | 294 | 3.16 |
    # | 286 | 3.66 |
    # | 288 | 3.44 |
    # | 1 | 3.88 |
    # | 300 | 3.63 |
    # | 121 | 3.44 |
    # +--------+--------+




    """
    1.agg: 是dataframe对象的API，作用是 在里面可以写多个聚合
    2.alias : 它是column对象的api，可以针对一个列 进行改名
    3.withColumnRename：他是dataframe对象的API，可以对df中的列进行改名，一次改一个列，改多个列可以链式调用
    4.orderBy：DataFrame对象api，参数1是参与排序的列，参数2是升序（True）或降序（False）
    5.first：DataFrame对象api，取出DF的第一行数据，返回结果是row对象。
    row对象就是一个数组，可以使用 row['字段名称']来取出某一列局的数值，返回的是数据，不是DataFrame
    """