##########################################################################################################################################
# File Name: LoadCSVWithSchema.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Reads a csv file example.csv and apply's a schema defined in code
##########################################################################################################################################

from utils import ContextProvider
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from decimal import Decimal

TestFilePath = "/TestData/example.csv"

def LoadData():
    print("> Loading date from CSV File")
    SparkContext = ContextProvider.getSparkInstance()
    Data = SparkContext.textFile(TestFilePath)
    DataFormatted = Data.map(lambda Line: Line.split(',')).map(lambda line: [line[0],line[1],int(line[2]),Decimal(line[3])])
    Schema = StructType( \
                          [StructField("State",StringType(),True), \
                           StructField("DealerID",StringType(),True), \
                           StructField("Deals",IntegerType(),True), \
                           StructField("Revenue",DecimalType(10,0),True)]
                         )
    print("> Applying predefined schema to CSV file date")
    DataSet = DataFormatted.toDF(Schema)
    return DataSet