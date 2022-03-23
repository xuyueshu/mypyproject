from pyspark import SparkContext,SparkConf

if __name__ == '__main__':
    conf=SparkConf().setMaster("local[*]").setAppName("test")
    sc=SparkContext(conf=conf)
    # 演示通过并行集合方式创建RDD，本地集合-》分布式RDD
    rdd=sc.parallelize([1,2,3,4,5,6,7,8,9])
    # parallelize方法，没有指定分区，默认的分区是多少？根据cpu核心来定
    # python字符串与不能与非字符串类型拼接，需要转换
    print("默认分区数：" + str(rdd.getNumPartitions()))

    ##设定分区
    rdd1=sc.parallelize([1,2,3,4,5,6],3)
    print("分区数："+ str(rdd1.getNumPartitions()))
    # collect方法是，将RDD（分布式对象）中的每个分区数据，都发送到Driver中，形成一个python List对象
    print("rdd的内容是："+str(rdd.collect()))

