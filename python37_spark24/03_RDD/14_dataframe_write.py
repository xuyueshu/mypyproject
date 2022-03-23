
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql import functions as F
import time
if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.\
        builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()

    """
    spark.sql.shuffle.partitions 参数是指，在sql计算中，shuffle算子截断默认的分区数是200个
    对于集群模式来说，200个也算合理
    如果在local下运行，200个很多，在调度上会带来额外的损耗
    所以在local下建议修改比较低  比如 2/4/6
    """


    sc=spark.sparkContext

    schema = StructType().\
        add("user_id",StringType(),nullable=True).\
        add("movie_id",StringType(),nullable=True).\
        add("rank",IntegerType(),nullable=True).\
        add("ts",StringType(),nullable=True)
    ##创建dataFrame
    df = spark.read.format("csv").\
        option("sep","\t").\
        option("header",False).\
        schema(schema).\
        load("../data/input/u.data")



    """df写出"""

    # wirte text ,只能写出一个列的数据，所以需要将dataframe转换成单列df
    df.select(F.concat_ws("---","user_id","movie_id","rank","ts")).\
        write.\
        mode("overwrite").\
        format("text").\
        save("../data/output/14_dataframe_write/text")

    #write csv
    df.write.mode("overwrite").\
        format("csv").\
        option("sep",";").\
        option("header",True).\
        save("../data/output/14_dataframe_write/csv")

    #  write json
    df.write.mode("overwrite").\
        format("json").\
        save("../data/output/14_dataframe_write/json")

    # write parquet
    df.write.mode("overwrite").\
        format("parquet").\
        save("../data/output/14_dataframe_write/parquet")
