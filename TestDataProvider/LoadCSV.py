##########################################################################################################################################
# File Name: LoadParquet.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Read a csv file exampl.csv and loads the data into a data frame 
##########################################################################################################################################

from utils import ContextProvider

TestFilePath = "/TestData/example.csv"

def LoadData():
    print("> Loading date from CSV File without any schema all fields defaulted to StringType")
    SparkContext = ContextProvider.getSparkInstance()
    Data = SparkContext.textFile(TestFilePath)
    DataSet = Data.map(lambda Line: Line.split(',')).toDF()
    DataSet = DataSet.withColumnRenamed("_1","State") \
                .withColumnRenamed("_2","DealerID") \
                .withColumnRenamed("_3","Deals") \
                .withColumnRenamed("_4","Revenue")
    return DataSet
