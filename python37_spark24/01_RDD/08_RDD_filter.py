from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    """
    保留满足条件的，函数返回值为bool
    """
    result=rdd.filter(lambda t: t%2==0)
    print(result.collect())
    """
    [2, 4, 6, 8, 10]
    """