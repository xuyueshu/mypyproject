
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # 通过sparksession创建sparkcontext
    sc=spark.sparkContext
    # 基于RDD转换成DataFrame
    file_rdd=sc.textFile("../data/input/01_dataframe_create_1.txt")
    # print(file_rdd.collect())
    rdd = file_rdd.map(lambda x : x.split(",")).map(lambda x: (x[0],int(x[1])))
    ##构建DataFrame对象
    # 参数1：被转换的rdd
    # 参数2：指定DataFrame的列名，即字段名称，按照顺序提供字符串名称
    df=spark.createDataFrame(rdd,schema=['name','age'])
    # 打印DataFrame表结构
    df.printSchema()
    # root
    #  |-- name: string (nullable = true)
    #  |-- age: long (nullable = true)


    # 打印该DataFrame中的数据
    # 参数1：展示多少条数据，默认20条
    # 参数2：表示对字段进行截断，如果字段长度超过20个字符串长度，后续的内容显示‘...’
    # False表示不截断全部显示，默认True
    df.show(2,False)

    # +----+---+
    # | name | age |
    # +----+---+
    # | 张三 | 20 |
    # | 李四 | 30 |
    # +----+---+

    # 将该DataFrame转换成临时的视图，供sql查询
    df.createOrReplaceTempView("people")
    spark.sql("select * from people where age<30").show()

    # +----+---+
    # | name | age |
    # +----+---+
    # | 张三 | 20 |
    # +----+---+
