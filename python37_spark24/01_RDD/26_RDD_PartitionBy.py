from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('hadoop',1),('spark',1),('hello',1),('flink',1),('hadoop',1),('spark',1)],2)
    print(rdd.glom().collect())
    # [[('hadoop', 1), ('spark', 1), ('hello', 1)], [('flink', 1), ('hadoop', 1), ('spark', 1)]]

    def process(k):
        if 'hadoop' == k or 'hello' ==k:
            return 0
        if 'spark' == k:
            return 1
        return 2


    print(rdd.partitionBy(3, process).glom().collect())
    # 重新分区后：[[('hadoop', 1), ('hello', 1), ('hadoop', 1)], [('spark', 1), ('spark', 1)], [('flink', 1)]]

    """
    partitionBy(参数1，参数2)
    -参数1 指定重新分区后的分区数
    -参数2 自定义分区规则，传一个函数
    参数2：是一个函数，（k）-> int
    一个传入参数进来，类型无所谓，但是返回值一定是int类型。将key传进来，写一个逻辑，决定返回一个分区编号。
    分区编号从0开始，不要超出分区数（参数1）-1
    """