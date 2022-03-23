from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('a',1),('b',2),('d',4),('c',3),('d',2),('c',1),('a',5),('b',5),('e',2),('f',12),('e',11)],3)
    """
    sortBy(func,acending:bool,numPartitions)
    1.参数1表示，给个函数按照指定的那个列进行排序
    2.参数2：Ture表示升序，False表示降序
    3.参数3：numPartitions表示用多少分区排序，要全局有序，numPartitions=1，否则是在各分区有序，全局无序
    """
    print(rdd.sortBy(lambda x:x[1],ascending=True,numPartitions=3).collect()) ##分区不为1
    # [('a', 1), ('c', 1), ('b', 2), ('d', 2), ('e', 2), ('c', 3), ('d', 4), ('a', 5), ('b', 5), ('e', 11), ('f', 12)]
    print(rdd.sortBy(lambda x:x[1],ascending=True,numPartitions=1).collect()) ##分区为1
    # [('a', 1), ('c', 1), ('b', 2), ('d', 2), ('e', 2), ('c', 3), ('d', 4), ('a', 5), ('b', 5), ('e', 11), ('f', 12)]

    print(rdd.sortBy(lambda x:x[0],ascending=False,numPartitions=1).collect())
    # [('f', 12), ('e', 2), ('e', 11), ('d', 4), ('d', 2), ('c', 3), ('c', 1), ('b', 2), ('b', 5), ('a', 1), ('a', 5)]