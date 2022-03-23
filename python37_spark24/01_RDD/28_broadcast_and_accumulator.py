import re

from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    abnormal_list=['#','!','%',',']
    ##将本地集合设置为广播变量
    broadcast=sc.broadcast(abnormal_list)
    ##开始sc中的一个累加器，初始值设为0.在整个分布式中设置的一个全局变量，而不是在各自分区各加各的
    acmlt=sc.accumulator(0)
    file_rdd=sc.textFile("../data/input/broadcast_and_accumulator.txt")
    #去掉空行，line.strip（）返回的有内容表示True，none就是False
    line_rdd = file_rdd.filter(lambda line: line.strip())
    print(line_rdd.collect())
    #去掉前后空格
    data_rdd = line_rdd.map(lambda line : line.strip())
    #按照正则表达式进行切割，‘\s+’表示一个或多个空格
    words_rdd = data_rdd.flatMap(lambda line : re.split("\s+",line))

    #返回一个bool
    def filter_func(word) :
        global acmlt
        if word in broadcast.value :
            #特殊字符计数
            acmlt +=1
            return False
        else:
            return True

    result = words_rdd.filter(filter_func).\
        map(lambda x : (x , 1)).\
        reduceByKey(lambda a, b: a + b).\
        collect()

    print("正常单词计数：",result)
    print("特殊字符计数：",acmlt)
