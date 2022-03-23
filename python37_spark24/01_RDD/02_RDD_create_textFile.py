from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)

    ##通过textFile读取数据
    file_rdd=sc.textFile("../data/002.txt")
    print("file_rdd的内容是：", file_rdd.collect())

