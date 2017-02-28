'''
Created on Dec 15, 2016

@author: toddc
'''
#T
from __future__ import print_function
import sys
from pyspark import SparkContext

def parseline(line): #Parses the line into columns and converts the numbers we need into ints.
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

sc = SparkContext(appName="Average")
    
lines = sc.textFile(sys.argv[1], 1)
map1 = lines.map(parseline)
#This maps the values into 1 instance for every value. We then reduce by key and count every instance of that key and add 
#the value that goes with each instance. Afterwards, we find the average of each key.
totals_by_age = map1.mapValues(lambda x: (x,1))\
.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averages_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
results = averages_by_age.collect()
for result in results:
    print(result)

#.reduceByKey(lambda x: x[1] / lines.count())
# find sum: map1 = (lines.map(lambda x: int(x)).reduce(add))