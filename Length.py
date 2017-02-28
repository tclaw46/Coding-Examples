'''
Created on Jan 25, 2017

@author: toddc
'''
from pyspark import SparkConf, SparkContext
from operator import add
import re
import sys
if __name__ == '__main__':

    def normalizeWords(text): #This is the regular expression used to split the amazon reviews into columns by comma.
        return re.compile(''',(?=(?:[^"]|'[^']*'|"[^"]*")*$)''', re.UNICODE).split(text)
    
    conf = SparkConf().setMaster("local").setAppName("Length")
    sc = SparkContext(conf = conf)
    
    lines = sc.textFile(sys.argv[1], 1) #Data is converted into an rdd called lines.
    rdd = lines.map(normalizeWords) #Splits the lines into columns.
    responses = rdd.map(lambda x: (x[0], x[9], (x[4], x[5]))) #Creates a new rdd with the information I needed. In this case
                                                             #I got the reviewer id, review, and helpful rating.
    filtered_responses = responses.filter(lambda x: (len(x[1]) > 300)) #This filters the rdd by length review in characters.
    reduced_responses = filtered_responses.map(lambda x: (x[2], 1))\
    .reduceByKey(add) ##Remaps the rdd by helpful rating and reduces by key to add every time a certain rating appeared.
    
    results = reduced_responses.collect()  #This is what I have so far. I will be modifying it according to Qualtrics needs.
    for result in results:
        print(result)