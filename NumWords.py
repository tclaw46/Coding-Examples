'''
Created on Jan 25, 2017

@author: toddc
'''
from pyspark import SparkConf, SparkContext
import csv
from operator import add
import re
if __name__ == '__main__':

    def normalizeWords(text):
        return re.compile(''',(?=(?:[^"]|'[^']*'|"[^"]*")*$)''', re.UNICODE).split(text)
    
    conf = SparkConf().setMaster("local").setAppName("Feedback")
    sc = SparkContext(conf = conf)
    lines = sc.textFile("C:/Users/toddc/OneDrive/Documents/GitHub/401_open_response/datasets/opt-out_feedback_sanitized.csv")
    test = lines.map(normalizeWords)
    responses = test.map(lambda x: (x[2], x[3])).mapValues(lambda x: len(x.split()))  
    filtered_responses = responses.filter(lambda x: (x[1] > 20))
    reduced_responses = filtered_responses.map(lambda x: (x[0], 1))\
    .reduceByKey(add)
    
    results = reduced_responses.collect()
    for result in results:
        print(result)