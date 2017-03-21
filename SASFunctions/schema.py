##########################################################################################################################################
# File Name: schema.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Reads the data provided and find if data is numeric, decimal or string
##########################################################################################################################################

from pyspark.sql.types import StringType, DecimalType, IntegerType

def GetSchema(DataSet):
    print("> Inferring schema from data (string, integer or decimal value)")
    for column in DataSet.columns:
        if DataSet.filter(DataSet[column].rlike("[^\d.+-]")).take(1) != []:
            DataSet = DataSet.withColumn(column, DataSet[column].cast(StringType()))
        elif DataSet.filter(DataSet[column].rlike("[^\d]")).take(1) != []:
            DataSet = DataSet.withColumn(column, DataSet[column].cast(DecimalType(10,2)))
        else:
            DataSet = DataSet.withColumn(column, DataSet[column].cast(IntegerType()))
    return DataSet