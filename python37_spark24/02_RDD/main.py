from pyspark import SparkContext,SparkConf
from pyspark.storagelevel import StorageLevel
from defs import context_jieba,filter_words,extract_user_and_words

if __name__ == '__main__':
    conf=SparkConf().setMaster("local[*]").setAppName("example01")
    sc=SparkContext(conf=conf)

    #读取数据文件
    file_rdd=sc.textFile("../data/input/SogouQ.txt")
    ##对数据进行切分
    split_rdd=file_rdd.map(lambda x: x.split("\t"))
    ##持久化
    split_rdd.persist(storageLevel=StorageLevel.DISK_ONLY)
    # print(split_rdd.takeSample(True, 3))
    ##TODO:需求1：用户搜索的关键词分析--关键词搜索频率
    #主要分析的热点词
    #将所有的搜索内容去除
    context_rdd=split_rdd.map(lambda x:str(x[2]).replace('[','').replace(']',''))
    # print(context_rdd.takeSample(True, 3))
    words_rdd=context_rdd.flatMap(context_jieba)
    # print(words_rdd.takeSample(True, 10))
    filter_rdd=words_rdd.filter(filter_words)
    # print(filter_rdd.takeSample(True, 40))
    result=filter_rdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(10)
    print(result)


    ##TODO:需求2：用户与关键词组合分析--每个用户关键词搜索频率
    user_content=split_rdd.map(lambda x: (x[1],str(x[2]).replace('[','').replace(']','')))
    user_word_with_one_rdd=user_content.flatMap(extract_user_and_words)
    result2=user_word_with_one_rdd.reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(10)
    print(result2)

    ##TODO:需求3：时间段分析
    result3=split_rdd.map(lambda x: (str(x[0]).split(":")[0],1)).\
        reduceByKey(lambda x,y:x+y).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1)\
    .take(5)
    print(result3)
    # split_rdd.unpersist()