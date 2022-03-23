from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,4,5,6,7])
    #方式一：定义一个方法，当作算子传入的参数
    def add(data):
        return data*10
    print(rdd.map(add).collect())

        # 方式二：通过lambda表达式
    print(rdd.map(lambda data:data*10).collect())
"""
lambda表达式适用于一行就搞定的函数体，如果是多行，还是需要定义独立的方法
"""



