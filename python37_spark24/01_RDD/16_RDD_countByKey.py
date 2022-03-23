from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd1=sc.textFile("..\data\countByKey.txt")
    rdd2=rdd1.flatMap(lambda line:line.split(" "))
    """
    countByKey按照key计数，active算子
    """
    result=rdd2.countByKey()
    print(result)
    print(type(result))

