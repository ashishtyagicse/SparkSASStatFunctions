##########################################################################################################################################
# File Name: LoadParquet.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Test module to run and show functionality of spark stats utility using example.csv and example.parquet files
##########################################################################################################################################

from TestDataProvider import LoadParquet, LoadCSVWithSchema, LoadCSV
from SASFunctions import AllStats
from SASFunctions.mean import getAllStats as MeanAllStats, getColumnStats as MeanColStats
from SASFunctions.freq import getAllStats as FreqAllStats, getColumnStats as FreqColStats

def RunTest():
    # Define Stats that need to be calculated and columns on which to calculate the stats 
    SchemaFromData = False  
    StatsTypeList = ['Frequency','Percentage'] 
    StringColumns = ["State", "DealerID"]
    NumericColumns = ["Deals", "Revenue"] 
        
    print("Utility to mimic SAS functions like proc freq and proc mean with combined output")
    print("Running test case one:")
    print("Test loads data from Parquet file with schema and calls the overall stats calculation module")
    DataFrame = LoadParquet.LoadData()
    AllStats.getAllStats(DataFrame, StatsTypeList , SchemaFromData).show()
     
    print("Running test case Two:")
    print("Test loads data from CSV file and predefined schema applied programmatically and calls All Stats modules for freq and mean stat modules")
    DataFrame = LoadCSVWithSchema.LoadData()
    FreqStats = FreqAllStats(DataFrame, StatsTypeList)
    MeanStats = MeanAllStats(DataFrame)
    FreqStats.show()
    MeanStats.show()
         
    print("Running test case Three:")
    print("Test loads data from CSV file with schema inferred from data itself and then calls frequency, percentage and mean functions using column level stats calculation functions")
    SchemaFromData = True
    DataFrame = LoadCSV.LoadData()
    FreqStats = FreqColStats(DataFrame, StringColumns, StatsTypeList)
    MeanStats = MeanColStats(DataFrame, NumericColumns)
    FreqStats.show()
    MeanStats.show()