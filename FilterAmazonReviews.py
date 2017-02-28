'''
Created on Jan 25, 2017

@author: toddc
'''
from __future__ import division #This file was used to Split Amazon reviews into training sets to test usefulness of 
from pyspark.sql import SparkSession #customer reviews. We split them based on number of people that had marked them helpful. 
import sys
   
spark = SparkSession.builder.config("spark.sql.warehouse.dir", \
"file:///C:/temp").appName("SparkSQL").getOrCreate()
lines = spark.sparkContext.textFile("C:/Users/toddc/Documents/Class/Winter 2017/CS40r/Reviews.csv")
header = lines.first() #Remove header from csv file to make results better.
lines = lines.filter(lambda x: x != header )
rdd = lines.map(lambda x: x.split(',')).filter(lambda x: x[5] != 0)\
.filter(lambda x: len(x) == 10) #Split columns by comma, then remove any review that had zero people that rated it or had messy data making it 
                                #difficult to use.
responses = rdd.map(lambda x: (str(x[1]), int(x[4]), int(x[5]), x[9])) #Product ID, how many people found a review helpful out of total people that rated the review, and the review itself.
reduced_responses1 = responses.filter(lambda x: x[2] >= 5 and (x[1]/x[2]) <= .33) #Reviews with more than 5 people rating it and less than 33% finding them helpful.
reduced_responses2 = responses.filter(lambda x: x[2] >= 5 and (x[1]/x[2]) > .33 and (x[1]/x[2]) <= .66) #Between 33% to 66% people finding these reviews helpful.
reduced_responses3 = responses.filter(lambda x: x[2] >= 5 and (x[1]/x[2]) > .66) #At least 66% people found these reviews helpful.

df1 = spark.createDataFrame(reduced_responses1)
df2 = spark.createDataFrame(reduced_responses2)
df3 = spark.createDataFrame(reduced_responses3)

df1.coalesce(1).write.format('com.databricks.spark.csv').save(sys.argv[2]) #Saved them to csv format.
df2.coalesce(1).write.format('com.databricks.spark.csv').save(sys.argv[3])
df3.coalesce(1).write.format('com.databricks.spark.csv').save(sys.argv[4])
