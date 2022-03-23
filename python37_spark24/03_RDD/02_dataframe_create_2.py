
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
   #### 使用structType来指定字段描述######
    schema = StructType().\
        add("name",data_type=StringType(),nullable=False).\
       add("age",data_type=IntegerType(),nullable=True)

    df = spark.createDataFrame(rdd,schema=schema)
    df.printSchema()

    # root
    # | -- name: string(nullable=false)
    # | -- age: integer(nullable=true)


    df.show()

    # +------+---+
    # | name | age |
    # +------+---+
    # | 张三 | 20 |
    # | 李四 | 30 |
    # | 王五 | 40 |
    # | 马六 | 50 |
    # | 刘德华 | 60 |
    # +------+---+