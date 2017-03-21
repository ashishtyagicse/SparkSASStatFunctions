##########################################################################################################################################
# File Name: LoadParquet.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Calculation module to calculate mean related stats on numeric columns
##########################################################################################################################################

from SASFunctions import schema
import pyspark.sql.functions as F
import json
    
# String constants 
NUM_STAT = "Number"
SUMMARY_COL = "summary"
COLUMN_NAME = "Column_Name"
COLUMN_VALUE = "Column_Value"
STATS_TYPE = "Stats_Type"
STATS_VALUE = "Stats_Value"

# Run describe on dataframe to get all mean related stats 
def getMean(DataSet, Column):
    Stats = DataSet.describe(Column)
    return Stats


# Calculate mean related stats per column based
def getColumnStats(DataSet, ColumnList):
    print("> Calculating mean stats for given columns")
    FirstRun = True
    ResultDataSet = None
    
    # Get mean stats and apply standard formatting and columns for output     
    for Column in ColumnList:
        Stats = getMean(DataSet, Column)
        TempStat = Stats.select(Column, SUMMARY_COL) \
                    .withColumnRenamed(Column, STATS_VALUE) \
                    .withColumnRenamed(SUMMARY_COL, STATS_TYPE) \
                    .withColumn(COLUMN_NAME, F.lit(Column)) \
                    .withColumn(COLUMN_VALUE, F.lit(NUM_STAT)) \
                    .select(COLUMN_NAME, COLUMN_VALUE, STATS_TYPE, STATS_VALUE)
        if FirstRun:
            ResultDataSet = TempStat
            FirstRun = False
        else:
            ResultDataSet = ResultDataSet.unionAll(TempStat)
    return ResultDataSet

# Calculate mean stats on all columns or coulmns specified. If schema is not present schema will be inferred from data itself 
def getAllStats(DataSet, SchemaFromData = False, Columns = []):
    print("> Computing mean stats for given data (all columns that match numeric data type only)")
    # Get schema from data 
    if SchemaFromData: 
        DataSet = schema.GetSchema(DataSet)
    
    NumericColumns = []
    # Identify which columns are numeric type 
    for Field in json.loads(DataSet.schema.json())["fields"]:
        DType = Field["type"]
        ColName = Field["name"]
        if Columns == [] or ColName in Columns:
            if "decimal" in DType or "float" in DType or "double" in DType or "integer" in DType or "long" in DType or "short" in DType :
                NumericColumns.append(ColName) 
    return getColumnStats(DataSet, NumericColumns)
