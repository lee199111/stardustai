
import os
import re
import pdb
import sys
import json
import heapq
import jieba
import glob
import requests
from collections import Counter
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from os import listdir
from os.path import isfile, join



def report(file,text_list, desc):
    '''检查语料的重复性，评价标准是：总句子数 / 总词数
    一般来说，这个指标达到0.6以下就可以接受了
    '''
    all_words = []
    for sent in text_list:
        sent = sent.lower()
        words = set(re.sub("[.?,'!]", '', word) for word in sent.split())
        # words = jieba.cut(sent)
        # print(words)
        all_words.extend(words)
    counter = Counter(all_words)
    score = len(text_list) / len(counter)
    print(key,'分数：%.3f'%(score), '共%d个句子，%d个词'%(len(text_list), len(counter)))
    return ["分数：{}, 句数：{}，词数：{}".format(score,len(text_list),len(counter))]


def get_data(path,num,file_name):
    data = {}
    for file in glob.glob(path):
        print("📛",file)
        with open(file,'r',encoding="utf-8") as f:
            lines = f.readlines()
            temp = []
            i = 0
            for line in lines[1:num*4:4]:
                temp.append(line.rstrip('\n'))
                i += 1
                # with open("/Users/lizhe/Desktop/qualified/123_{file_name}.txt".format(file_name=file_name),'a',encoding="utf-8") as f:
                    # f.write(line)
                # print(line)
            # print("🚱",i)
        data[file.split("/")[-1]] = temp
    return data

if __name__ == '__main__':
    # file_name = "Italian_RMDM_XC_XLJ_20210820_1"
    file_name = "*"
    num = 1000   
    
    path = "/Users/lizhe/Desktop/qualified/{file_name}.txt".format(file_name=file_name)
    result = {}
    data = get_data(path,num=num,file_name=file_name)
    for key,value in data.items():
        result[key]=report(key,value,desc="???")
    print(result)
        

