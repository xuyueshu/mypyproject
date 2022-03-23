from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('a',1),('b',1),('c',1),('a',1),('c',0),('a',3)])
    """
    通过groupBy对数据分组
    groupBy传入的函数意思是：通过这个函数，确定按照谁来分组（返回谁即可）
    分组的规则和sql是一致的，也就是相同的在一个组（hash分组）
    """
    rdd1=rdd.groupBy(lambda t:t[0])
    print(rdd1.collect())
    ##打印出rdd1的第二元素为一个可以迭代的对象
    """
    [('b', <pyspark.resultiterable.ResultIterable object at 0x0000013BCE708448>), 
    ('c', <pyspark.resultiterable.ResultIterable object at 0x0000013BCE708408>),
     ('a', <pyspark.resultiterable.ResultIterable object at 0x0000013BCE708488>)]
    """
    result=rdd1.map(lambda t: (t[0],list(t[1]))) #将第二个元素ResultIterable强制转换成list

    print(result.collect())