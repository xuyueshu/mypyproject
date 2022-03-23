from pyspark import SparkConf,SparkContext
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    score_info_rdd = sc.parallelize([
        (1, "语文", 125),
        (2, "数学", 145),
        (3, "物理", 110),
        (4, "外语", 125),
        (1, "外语", 145),
        (2, "物理", 110),
        (3, "化学", 110),
        (4, "数学", 109),
        (1, "物理", 118),
        (2, "化学", 127),
        (3, "数学", 123),
        (4, "语文", 119)
    ])

    #####################1.不使用广播变量########################

    stu_info_list1 = [
        (1, "王力宏", 25),
        (2, "周杰伦", 28),
        (3, "王杰",   35),
        (4, "刘德军", 45),
        (5, "张学友", 27),
        (6, "谢霆锋", 15)
    ]

    def map_func1(data):
        id = data[0]
        stu_name=""

        for stu in stu_info_list1:
            if id == stu[0]:
                stu_name=stu[1]

        return (stu_name,data[1],data[2])


    ##此处关联本地集合，是将本地集合远程io发送到所有excutor下的所有分区。一个excutor可以有多个分区，所以会发送多次  --  1个excutor 多份数据
    print(score_info_rdd.map(map_func1).collect())

    #####################1.使用广播变量########################

    stu_info_list2 = [
        (1, "王力宏", 25),
        (2, "周杰伦", 28),
        (3, "王杰",   35),
        (4, "刘德军", 45),
        (5, "张学友", 27),
        (6, "谢霆锋", 15)
    ]

    ##将本地集合使用广播变量的方式发送到所有的excutor，一个excutor只发送一次，所有分区从所属的excutor处获取----- 1个excutor 1份数据
    broadcast = sc.broadcast(stu_info_list2)
    def map_func2(data):
        id = data[0]
        stu_name=""

        for stu in broadcast.value: ##从broadcast.value中获取
            if id == stu[0]:
                stu_name=stu[1]

        return (stu_name,data[1],data[2])


    print(score_info_rdd.map(map_func2).collect())


    """
    场景：本地集合对象 和分布式集合对象（RDD）进行关联的时候
    需要将本地 集合对象，封装成广播变量
    可以节省：
    1.网络IO的次数
    2.Excutor的内存占用
    """