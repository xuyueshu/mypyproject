
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType
if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # 通过sparksession创建sparkcontext
    sc=spark.sparkContext

    ##
    df = spark.read.format("csv").\
        option("sep",";").\
        option("header",True).\
        schema("name STRING,age INT,gender STRING").\
        load("../data/input/07_dataframe_create_5_csv.csv")

    df.printSchema()

    df.show()

    # root
    # | -- name: string(nullable=true)
    # | -- age: integer(nullable=true)
    # | -- gender: string(nullable=true)
    #
    # +------+----+------+
    # | name | age | gender |
    # +------+----+------+
    # | 张三 | 20 | man |
    # | 李四 | null | woman |
    # | 王五 | 27 | man |
    # | null | 33 | woman |
    # | 张学友 | 40 | null |
    # +------+----+------+

