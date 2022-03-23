from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([12,2,13,4,15,6,7,8,9,10,11],1)
    """
    takeOrdered(参数1，参数2)
    1.参数1 取几个数据
    2.参数2 对飘絮的数据进行更改（不会更改数据本身，只是在排序的时候使用下）
    默认按照自然升序排序，要想倒叙，对参数二处理
    """
    # 升序
    print(rdd.takeOrdered(3))
    # 降序
    print(rdd.takeOrdered(3, lambda x: -x))