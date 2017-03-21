##########################################################################################################################################
# File Name: LoadParquet.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Reads a Parquet file example.parquet and load it in a data frame
##########################################################################################################################################

from utils import ContextProvider

TestFilePath = "/TestData/example.parquet"

def LoadData():
    print("> Loading date from Parquet File")
    SqlContext = ContextProvider.getSQLContext()
    DataSet = SqlContext.read.parquet(TestFilePath)
    return DataSet