from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,4,5,6,7,8,9,10,11],1)
    """
    foreach 是对每个元素执行指定的操作（类似map），只是没有返回值
    直接在excutor上操作了，不会collect到driver中。
    比如向数据库中插入数据，省去了到driver的步骤，效率更高
    """
    rdd.foreach(lambda x:print(x))
