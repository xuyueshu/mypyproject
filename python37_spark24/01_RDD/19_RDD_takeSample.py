from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,4,5,6,7,8,9,10,11],1)
    """
    参数1：True表示可以取同一个数据，False表示不允许去同一个数据（有无放回抽样）
    参数2：抽样的个数
    参数3：随机数种子，传入一个数字即可，随意给,数字相当于是步长，当参数1为false时，同样的种子，取出来的是一样的
    """
    print(rdd.takeSample(False, 5,2))

