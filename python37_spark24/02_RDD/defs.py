import jieba
def context_jieba(data):
    return jieba.cut_for_search(data)

def filter_words(data):
    return data not in ["+","/","com","cn",'.']

def extract_user_and_words(data):
    userid=data[0]
    content=data[1]
    words=context_jieba(content)
    return_list=list()
    for word in words:
        if filter_words(word):
            return_list.append((userid+"_"+word,1))
    return return_list