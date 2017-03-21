##########################################################################################################################################
# File Name: LoadParquet.py
# Author: Ashish Tyagi
# Date created: March 17, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Calculation module to calculate all stats on given dataset 
##########################################################################################################################################

from SASFunctions import schema
from SASFunctions.mean import getColumnStats as MeanStats
from SASFunctions.freq import getColumnStats as FreqStats
import json

# Calls correct module required to get the stats
def getAllStats(DataSet, StatsTypeList, SchemaFromData = False, Columns = []):
    print("> Computing all stats for given data")
    # Get schema from data
    if SchemaFromData: 
        DataSet = schema.GetSchema(DataSet)
    
    StringColumns = []
    NumericColumns = []
    FreqStat = None
    MeanStat = None
    # Identify which columns are string and which are numeric type 
    for Field in json.loads(DataSet.schema.json())["fields"]:
        DType = Field["type"]
        ColName = Field["name"]
        if Columns == [] or ColName in Columns: 
            if "decimal" in DType or "float" in DType or "double" in DType or "integer" in DType or "long" in DType or "short" in DType:
                NumericColumns.append(ColName)
            else :
                StringColumns.append(ColName) 
    
    # Calls frequency and mean stats modules for required columns
    if StringColumns != []:
        FreqStat = FreqStats(DataSet, StringColumns, StatsTypeList)
    if NumericColumns != []:
        MeanStat = MeanStats(DataSet, NumericColumns)
    if FreqStat != None and MeanStat != None:
        ResultStats = FreqStat.unionAll(MeanStat)
    elif FreqStat != None:
        return FreqStat
    else:
        return MeanStat
    return ResultStats
    