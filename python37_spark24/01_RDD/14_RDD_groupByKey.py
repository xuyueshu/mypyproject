from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd1=sc.parallelize([(1,'a'),(2,'b'),(1,'c'),(2,'d')])
    """
    groupByKey针对kv型的rdd，自动按照key分组
    """
    result1=rdd1.groupByKey()
    print(result1.collect())
    # [(1, <pyspark.resultiterable.ResultIterable object at 0x00000238CBDC36C8>), (2, <pyspark.resultiterable.ResultIterable object at 0x00000238CBDC3688>)]
    print(result1.map(lambda x:(x[0],list(x[1]))).collect())
    # [(1, ['a', 'c']), (2, ['b', 'd'])]

    """
    对比groupBy与groupByKey
    1.groupBy要指定以哪个分组，groupByKey自动按照key分组
    2.groupBy collect后生成的元组中的list中同时有key和value，groupByKeycollect后生成的元组中的list只包含value
    """
    rdd2=sc.parallelize([(1,'a'),(2,'b'),(1,'c'),(2,'d')])
    result2=rdd2.groupBy(lambda x:x[0])
    print(result2.collect())
    # [(1, <pyspark.resultiterable.ResultIterable object at 0x0000015658CB6608>), (2, <pyspark.resultiterable.ResultIterable object at 0x0000015658CB65C8>)]
    print(result2.map(lambda x:(x[0],list(x[1]))).collect())
    # [(1, [(1, 'a'), (1, 'c')]), (2, [(2, 'b'), (2, 'd')])]