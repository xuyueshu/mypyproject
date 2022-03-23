
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType
if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # 通过sparksession创建sparkcontext
    sc=spark.sparkContext

    ##json数据会自动推断出字段名称和类型
    df = spark.\
        read.\
        format("json").\
        load("../data/input/06_dataframe_create_5_json.txt")

    df.printSchema()

    df.show()

    # root
    # | -- age: long(nullable=true)
    # | -- name: string(nullable=true)
    #
    # +----+----+
    # | age | name |
    # +----+----+
    # | 20 | 张三 |
    # | 30 | 李四 |
    # | null | 王五 |
    # | 50 | null |
    # +----+----+
