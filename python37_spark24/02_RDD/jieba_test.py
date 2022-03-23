# coding:utf8
import jieba

if __name__ == '__main__':
    content="小明硕士毕业于中国科学院计算所，后在清华大学深造"
    result1=jieba.cut(content,cut_all=True) ##cut_all=True 全模式
    #print(result1)
    #print(type(result1))##<class 'generator'> 可以迭代的对象，需要转换成list
    print(list(result1))
    # ['小', '明', '硕士', '毕业', '于', '中国', '中国科学院', '科学', '科学院', '学院', '计算', '计算所', '，', '后', '在', '清华', '清华大学', '华大', '大学', '深造']
    result2=jieba.cut(content,cut_all=False) ##精准模式
    print(list(result2))
    # ['小明', '硕士', '毕业', '于', '中国科学院', '计算所', '，', '后', '在', '清华大学', '深造']

#     搜索引擎模式，等同于允许二次组合的场景
    content3=jieba.cut_for_search(content)
    print(",".join(content3))##用‘，’隔开打印输出

