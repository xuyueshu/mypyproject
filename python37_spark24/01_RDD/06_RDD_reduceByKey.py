from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('a',1),('b',1),('c',1),('a',1),('c',1)])
    # reduceByKey就是对相同key的进行聚合，对数据相加
    rdd2=rdd.reduceByKey(lambda a,b: a+b)
    print(rdd2.collect())
    """
    [('b', 1), ('c', 2), ('a', 2)]
    """