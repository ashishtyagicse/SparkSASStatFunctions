#######################################################################################################################################
# File Name: ContextProvider.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Creates or reuses the spark context, SQL context  
#######################################################################################################################################

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

sparkContext = None
sqlContext = None
sparkStreamingContext = None

# Creates a spark context if not exists already
def getSparkInstance():
    global sparkContext
    if sparkContext == None:
        # Create Spark Context
        conf = SparkConf().setAppName("Spark Stats Job")
        # for local mode use following 
        #.setMaster(ExecutionMode).set("spark.executor.instances", 3).set("spark.local.ip", "127.0.0.1")
        sparkContext = SparkContext(conf=conf)
    return sparkContext

# Checks if spark context is present if not creates it and then creates or reuses the SQL context
def getSQLContext():
    global sparkContext
    global sqlContext
    if sparkContext == None:
        getSparkInstance()
    if sqlContext == None:
        sqlContext = SQLContext(sparkContext)
    return sqlContext
