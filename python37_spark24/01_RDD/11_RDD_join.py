from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd1=sc.parallelize([(1001,"zhangsan"),(1002,"lisi"),(1003,"wangwu"),(1004,"maliu")])
    rdd2=sc.parallelize([(1001,"销售部"),(1004,"人事部")])
#     join 会按照二元元组的key关联
    print(rdd1.join(rdd2).collect())
    """
    [(1001, ('zhangsan', '销售部')), (1004, ('maliu', '人事部'))]
    """
#     leftOuterJoin
    print(rdd1.leftOuterJoin(rdd2).collect())
    """
    [(1001, ('zhangsan', '销售部')), (1002, ('lisi', None)), (1003, ('wangwu', None)), (1004, ('maliu', '人事部'))]
    """
#     rightOuterJoin
    print(rdd1.rightOuterJoin(rdd2).collect())
    """
    [(1001, ('zhangsan', '销售部')), (1004, ('maliu', '人事部'))]
    """

