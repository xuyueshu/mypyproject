"""
需求:读取data文件夹中的ordertext文件,提取北京的数据,组合北京和商品类别进行输出同时对结果集进行去重,得到北京售卖的商品类别信息
"""
from pyspark import SparkConf,SparkContext
import json
if __name__ == '__main__':

    #构建SparkContext对象,local[*]表示本地所有的资源
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    file_rdd=sc.textFile("../data/order.txt")
    # 按照‘|’切割，得到一个个的json字符串
    json_rdd=file_rdd.flatMap(lambda line:line.split("|"))
    # print(json_rdd.collect())
    # 通过python json内库，完成json字符串到字典对象的转换
    dict_rdd=json_rdd.map(lambda json_str:json.loads(json_str))
    # print(dict_rdd.collect())
    # 过滤出只含北京的dict
    beijing_rdd=dict_rdd.filter(lambda x:x['areaName']=="北京")
    # 将北京和商品类型拼接
    # print(beijing_rdd.collect())
    category_rdd=beijing_rdd.map(lambda x:x['areaName']+"_"+x['category'])
    # print(category_rdd.collect())
    # 去重
    result_rdd=category_rdd.distinct()
    print(result_rdd.collect())
