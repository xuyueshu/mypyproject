
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType

if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # 通过sparksession创建sparkcontext
    sc=spark.sparkContext
    # 基于RDD转换成DataFrame
    file_rdd=sc.textFile("../data/input/01_dataframe_create_1.txt")
    # print(file_rdd.collect())
    rdd = file_rdd.map(lambda x : x.split(",")).map(lambda x: (x[0],int(x[1])))

    """使用toDF两种方式来构建DataFrame"""
    #方式1 靠推断，默认可为空
    df1=rdd.toDF(["name","age"])
    df1.printSchema()
    df1.show()

    #方式2 通过structType
    schema = StructType(). \
        add("name", data_type=StringType(), nullable=False). \
        add("age", data_type=IntegerType(), nullable=True)
    df2=rdd.toDF(schema=schema)
    df2.printSchema()
    df2.show()
