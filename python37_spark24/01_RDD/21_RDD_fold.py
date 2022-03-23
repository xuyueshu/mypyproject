from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)

    """
    fold
    1.是一个active算子
    2.功能和reduce一样，会接受一个初始值进行聚合。
    3.分区内聚合及分区间聚合
    """
    # 1.当分区不为1时
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)
    print(rdd.glom().collect()) #查看下分区是怎么分布的
    # [[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]]

    result=rdd.fold(10,lambda x,y:x+y)
    print(result)
    print(type(result)) # 结果95  即[10，[10，1, 2, 3], [10，4, 5, 6], [10，7, 8, 9, 10]]

    # 1.当分区为1时
    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1)
    print(rdd1.glom().collect()) #查看下分区是怎么分布的
    # [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]

    result1=rdd1.fold(10,lambda x,y:x+y) #结果75 即[10，[10，1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]
    print(result1)
    print(type(result1))
