from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([12,2,13,4,15,6,7,8,9,10,11],3)
    """
    saveAsTextFile
    1.可以向本地或hdfs输出文件
    2.由excutor执行，有多少个excutor（分区）就有多少个文件
    """
    rdd.saveAsTextFile("../data/output/saveAsTextFile")
    """
    在众多的action算子中，foreach和saveAsTextFile是分区（excutor）直接执行的，
    跳过了Driver，其他action算子需要将结果发送到Driver中
    """
