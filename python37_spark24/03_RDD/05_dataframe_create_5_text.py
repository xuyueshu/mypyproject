
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType
if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # 通过sparksession创建sparkcontext
    sc=spark.sparkContext
    """统一读取构建DataFrame 的api"""
    ##构建StructType对象，text数据源，读取数据的特点是，
    # 将一整行只作为‘一个字段’读取，默认列名是value，类型是string
    schema = StructType().add("data",StringType(),nullable=True)
    df = spark.read.format("text").\
        schema(schema).\
        load("../data/input/01_dataframe_create_1.txt")

    df.printSchema()
    df.show()

    # root
    # | -- data: string(nullable=true)
    #
    # +---------+
    # | data |
    # +---------+
    # | 张三, 20 |
    # | 李四, 30 |
    # | 王五, 40 |
    # | 马六, 50 |
    # | 刘德华, 60 |
    # +---------+

