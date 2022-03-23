from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd1=sc.parallelize([('a',1),('b',2),('c',1),('m',1,'b',2)])
    rdd2=sc.parallelize([('a',2),('b',1),('c',1),('m',1,'b',2)])
    """
    intersection取交集，取两个rdd之间完全一样的部分
    """
    print(rdd1.intersection(rdd2).collect())

    """
    [('m', 1, 'b', 2), ('c', 1)]
    """