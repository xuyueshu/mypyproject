from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,4,5,6])
    """
    reduce(func(T,T)->T)
    1.是一个active算子
    2.输入和输出保持同样的类型
    """
    result=rdd.reduce(lambda x,y:x+y)
    print(result)
    print(type(result))
