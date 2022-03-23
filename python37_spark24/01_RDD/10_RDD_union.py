from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd1=sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    rdd2=sc.parallelize([1,2,3,20,40])
    print(rdd1.union(rdd2).collect())
"""
1.union不会去重
2.不同类型的也可以union
"""