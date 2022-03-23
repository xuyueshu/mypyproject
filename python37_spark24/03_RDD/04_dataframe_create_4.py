
from pyspark.sql import SparkSession
import pandas as pd
if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # 通过sparksession创建sparkcontext
    sc=spark.sparkContext

    """利用第三方库pandas的DataFrame来构建spark的 DataFrame"""

    pddf=pd.DataFrame(
        {
            "id": [1,2,3],
            "name" : ["张三","李四","王五"],
            "age" : [20,30,40]
        }
    )
    # 将pandas的DataFrame 转换成spark的 DataFrame
    df = spark.createDataFrame(pddf)
    df.printSchema()
    df.show()

    # root
    #  |-- id: long (nullable = true)
    #  |-- name: string (nullable = true)
    #  |-- age: long (nullable = true)
    #
    # +---+----+---+
    # | id|name|age|
    # +---+----+---+
    # |  1|张三| 20|
    # |  2|李四| 30|
    # |  3|王五| 40|
    # +---+----+---+


