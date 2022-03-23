from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([12,2,13,4,15,6,7,8,9,10,11],3)
    def process(iter):
        result=list()
        for i in iter:
            result.append(i*10)
        return result


    print(rdd.mapPartitions(process).collect())

    """
    mapPartitions() 接受的是一个分区的数据，作为一个迭代器（一次性list）对象传过来
    与map的不同：map是一条一条的处理，每一条就是一个io，mapPartitions接受的是一个分区打包的list，减少了io，更快
    """