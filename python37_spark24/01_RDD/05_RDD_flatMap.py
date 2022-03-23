from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize(["hello java","hello scala","hello python","hello spark"])
    print(rdd.map(lambda line: line.split(" ")).collect())
    """
    [['hello', 'java'], ['hello', 'scala'], ['hello', 'python'], ['hello', 'spark']]
    """
    print(rdd.flatMap(lambda line: line.split(" ")).collect())
    # 相当于在map上的解套操作
    """
    ['hello', 'java', 'hello', 'scala', 'hello', 'python', 'hello', 'spark']
    """

