
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType
from pyspark.sql import functions as F
if __name__ == '__main__':
    # 创建sparkSession
    spark= SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext

    #TODO 1:SQL风格进行处理


    # dataFrame需要的是[[],[],[]],所以需要在每个上套一个[]
    rdd = sc.textFile("../data/input/words.txt").\
        flatMap(lambda x : str(x).
                split(" ")).map(lambda x : [x])

    df = rdd.toDF(["word"])
    df.createOrReplaceTempView("words")

    spark.sql("select word,count(*) as cnt from  words group by word order by cnt desc ").show()

    # +------+---+
    # | word | cnt |
    # +------+---+
    # | hello | 5 |
    # | spark | 1 |
    # | world | 1 |
    # | scala | 1 |
    # | java | 1 |
    # | python | 1 |
    # +------+---+

    #TODO 2:采用DSL方式
    #
    #withColumn 对参数所在列进行操作，返回一个新的列，如果名字和老列相同，那么替换，否则作为新列存在
    df1 = spark.read.format("text").load("../data/input/words.txt") ##将每一行都读取到默认的value字段中
    df2 = df1.withColumn("value", F.explode(F.split(df1['value'], " ")))
    df2.groupBy("value").\
        count().\
        withColumnRenamed("value","word").\
        withColumnRenamed("count","cnt").\
        orderBy("cnt",ascending=False).\
        show()

    # +------+---+
    # | word | cnt |
    # +------+---+
    # | hello | 5 |
    # | spark | 1 |
    # | world | 1 |
    # | scala | 1 |
    # | java | 1 |
    # | python | 1 |
    # +------+---+
