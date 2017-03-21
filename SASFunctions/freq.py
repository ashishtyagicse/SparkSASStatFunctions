##########################################################################################################################################
# File Name: LoadParquet.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Calculation module to calculate mean related stats on string columns 
##########################################################################################################################################

from SASFunctions import schema
from pyspark.sql.types import DecimalType
import pyspark.sql.functions as F
import json

# String constants 
FREQ_STAT = "Frequency"
PERCENT_STAT = "Percentage"
COLUMN_NAME = "Column_Name"
COLUMN_VALUE = "Column_Value"
STATS_TYPE = "Stats_Type"
STATS_VALUE = "Stats_Value"

# Get the count as frequency for each column
def getFreq(DataSet, Column):
    Stats = DataSet.groupBy(Column).count() \
            .withColumnRenamed("count", FREQ_STAT)
    return Stats

# Calculate the percentage for each column
def getPercentage(DataSet, Column):
    Total = DataSet.count()
    Stats = getFreq(DataSet, Column) \
            .withColumn(PERCENT_STAT, \
                       F.expr("(" + FREQ_STAT + " * 100)/" + str(Total)) \
                       .cast(DecimalType(10,2)))
    return Stats

# Calculate Frequency related stats per column based
def getColumnStats(DataSet, ColumnList, StatTypeList):
    print("> Calculating freq and percentage stats for given columns")
    FirstRun = True
    ResultDataSet = None
    
    FreqStatFlag = False
    PercentStatFlag = False
    
    # if there is no list provided which type of stats to process then its a error
    if StatTypeList == []:
        print "Internal processing error: No stats to compute"
        return None
    
    # Set flags for which type of stats to run 
    for Stats in StatTypeList:
        if Stats == FREQ_STAT:
            FreqStatFlag = True
        elif Stats == PERCENT_STAT:
            PercentStatFlag = True
    
    # Get Frequency stats and apply standard formatting and columns for output 
    for Column in ColumnList:
        Stats = getPercentage(DataSet, Column)
        if FreqStatFlag:
            StatsFreq = Stats.select(Column,FREQ_STAT) \
                        .withColumnRenamed(Column, COLUMN_VALUE) \
                        .withColumnRenamed(FREQ_STAT, STATS_VALUE) \
                        .withColumn(COLUMN_NAME, F.lit(Column)) \
                        .withColumn(STATS_TYPE, F.lit(FREQ_STAT)) \
                        .select(COLUMN_NAME, COLUMN_VALUE, STATS_TYPE, STATS_VALUE)
        if PercentStatFlag:
            StatsPercent = Stats.select(Column,PERCENT_STAT) \
                        .withColumnRenamed(Column, COLUMN_VALUE) \
                        .withColumnRenamed(PERCENT_STAT, STATS_VALUE) \
                        .withColumn(COLUMN_NAME, F.lit(Column)) \
                        .withColumn(STATS_TYPE, F.lit(PERCENT_STAT)) \
                        .select(COLUMN_NAME, COLUMN_VALUE, STATS_TYPE, STATS_VALUE)
        if FirstRun:
            if FreqStatFlag and PercentStatFlag:
                ResultDataSet = StatsFreq.unionAll(StatsPercent)
            elif FreqStatFlag:
                ResultDataSet = StatsFreq
            elif PercentStatFlag:
                ResultDataSet = StatsPercent
            FirstRun = False
        else:
            if FreqStatFlag and PercentStatFlag:
                ResultDataSet = ResultDataSet.unionAll(StatsFreq.unionAll(StatsPercent))
            elif FreqStatFlag:
                ResultDataSet = ResultDataSet.unionAll(StatsFreq)
            elif PercentStatFlag:
                ResultDataSet = ResultDataSet.unionAll(StatsPercent)
    return ResultDataSet

# Calculate frequency stats on all columns or coulmns specified. If schema is not present schema will be inferred from data itself         
def getAllStats(DataSet, StatsTypeList, SchemaFromData = False, Columns = []):
    print("> Computing freq stats for given data (allColumns that match string type only)")
    # Get schema from data 
    if SchemaFromData: 
        DataSet = schema.GetSchema(DataSet)
    
    StringColumns = []
    # Identify which columns are String type 
    for Field in json.loads(DataSet.schema.json())["fields"]:
        DType = Field["type"]
        ColName = Field["name"]
        if Columns == [] or ColName in Columns:
            if not ("decimal" in DType or "float" in DType or "double" in DType or "integer" in DType or "long" in DType or "short" in DType) :
                StringColumns.append(ColName) 
    return getColumnStats(DataSet, StringColumns, StatsTypeList)
