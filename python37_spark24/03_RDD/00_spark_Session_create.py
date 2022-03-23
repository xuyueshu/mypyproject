#导包，sparksession对象来源于pyspark.sql包
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 创建sparksession对象
    spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()

    # 通过sparksession对象获取sparkcontext
    sc=spark.sparkContext
#     sparkSql
    #读取一个结构化数据文件
    df = spark.read.csv("../data/input/00_spark_Session_create.txt")
    df1=df.toDF("id","name","score")
    #打印下结构
    df1.printSchema()

    # root
    #  |-- id: string (nullable = true)
    #  |-- name: string (nullable = true)
    #  |-- score: string (nullable = true)

    #查看数据
    df1.show()

    #     +---+----+-----+
    # | id|name|score|
    # +---+----+-----+
    # |  1|语文|  101|
    # |  1|数学|  110|
    # |  1|外语|  123|
    # |  1|物理|  112|
    # |  1|生物|   66|
    # |  1|化学|   98|
    # |  2|语文|  102|
    # |  2|数学|  111|
    # |  2|外语|  124|
    # |  2|物理|  109|
    # |  2|生物|   67|
    # |  2|化学|   90|
    # |  3|语文|  104|
    # |  3|数学|  113|
    # |  3|外语|  114|
    # |  3|物理|  107|
    # |  3|生物|   65|
    # |  3|化学|   92|
    # +---+----+-----+


    #创建临时视图
    df1.createTempView("score")

    #sql风格查询
    spark.sql(
        """
        select * from score where id='1' limit 4
        """
    ).show()

    # +---+----+-----+
    # | id | name | score |
    # +---+----+-----+
    # | 1 | 语文 | 101 |
    # | 1 | 数学 | 110 |
    # | 1 | 外语 | 123 |
    # | 1 | 物理 | 112 |
    # +---+----+-----+

    # DSL风格
    df1.where("name='语文'").limit(5).show()

    # +---+----+-----+
    # | id | name | score |
    # +---+----+-----+
    # | 1 | 语文 | 101 |
    # | 2 | 语文 | 102 |
    # | 3 | 语文 | 104 |
    # +---+----+-----+


    """
    SparkSql中的数据抽象为DataFrame（python，R，java，scala），DataSet（java，scala，需要泛型支持），都为分布式数据集
    DataFrame中储存的数据结构是以表格形式组织的，方便sql计算
    """



    """
    基于这个前提，DataFrame的组成如下:
        在结构层面:
        - StructType对象描述整个DataFrame的表结构- StructField对象描述一个列的信息
        在数据层面
        - Row对象记录一行数据
        - Column对象记录一列数据并包含列的信息
    """