from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("woldCount")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # file_rdd = sc.textFile("file:\\\D\pycharmProject\pyspark\00_example\data\wordCount.txt")
    data = ["hello", "world", "hello", "word", "count", "count", "hello"]
    file_rdd=sc.parallelize(data)
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    word_with_one_rdd = words_rdd.map(lambda x: (x, 1))
    result_rdd = word_with_one_rdd.reduceByKey(lambda v1, v2: v1+v2)
    result_rdd.foreach(lambda s : print(s))