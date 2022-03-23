from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,12,23],3)
    """
    glom将RDD的数据加上嵌套，这个嵌套按照分区来进行
    """
    print(rdd1.glom().collect())
    """
    [[1, 2, 3], [4, 5, 6], [7, 8, 12, 23]]
    """
    rdd2 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 12, 23], 5)
    print(rdd2.glom().collect())
    """
    [[1, 2], [3, 4], [5, 6], [7, 8], [12, 23]]
    """
    rdd3 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 12, 23], 5)
    # 在glom基础上解套,flatMap中的函数传空实现
    print(rdd3.glom().flatMap(lambda x:x).collect())
    """
    [1, 2, 3, 4, 5, 6, 7, 8, 12, 23]
    """