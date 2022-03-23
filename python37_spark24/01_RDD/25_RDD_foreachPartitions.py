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
        print(result)


    rdd.foreachPartition(process)

    """
    跟mapPartitions 原理一样，一次处理一个分区的数据
    """