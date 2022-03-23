from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('A',1),('b',2),('d',4),('C',3),('d',2),('c',1),('a',5),('B',5),('e',2),('f',12),('E',11)],3)
    """
    sortByKey(acending=bool,numPartitions=None,keyfunc=<function RDD.<lambda>>)，针对kv型rdd按照key进行排序
    1.参数1表示，Ture表示升序，False表示降序 
    2.参数2：numPartitions表示用多少分区排序，要全局有序，numPartitions=1，否则是在各分区有序，全局无序
    3.参数3：排序前针对key进行处理，语法是：（k）->U,一个参数传入，返回一个值
    """
    print(rdd.sortByKey(ascending=False, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())
#     [('f', 12), ('e', 2), ('E', 11), ('d', 4), ('d', 2), ('C', 3), ('c', 1), ('b', 2), ('B', 5), ('A', 1), ('a', 5)]
