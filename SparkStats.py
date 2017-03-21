########################################################################################################################################
# File Name: SparkGetStats.py
# Author: Ashish Tyagi
# Date created: March 15, 2017
# Date last modified: March 20, 2017
# Python Version: 2.7
# Description: Starts a spark application which can data from HDFS and apply calculations to mimic SAS PROC FREQ and PROC MEAN functions 
########################################################################################################################################
import sys
from utils import ContextProvider
from SASFunctions import AllStats
from SASFunctions.mean import getAllStats as MeanAllStats
from SASFunctions.freq import getAllStats as FreqAllStats, getColumnStats as FreqColStats
from TestModule import RunTest

# Save the resulting data in a parquet or csv file
def Save(ResultDataSet, SaveResultFilePath, SaveResultType, SaveSplitter):
    if SaveResultType == "parquet":
        ResultDataSet.write.parquet(SaveResultFilePath)
    elif SaveResultType == "csv":
        ResultDataSet.rdd.map(lambda x: SaveSplitter.join(map(str, x))).coalesce(1).saveAsTextFile(SaveResultFilePath)
    
if __name__ == '__main__':
    # Check if required parameters are passed  
    if sys.argv[1] == '-h':
        print 'usage:' , sys.argv[0] , 'option' 
        print 'options:'
        print '-allstats' , 'Compute all required stats on given dataset'
        print '-freq:<option>' , 'Calculate freq stats for given dataset'
        print '-freq:all' , 'All calculations in frequency module'
        print '-freq:count', 'Calculates count on all non numeric columns'
        print '-freq:percent' , 'Calculates count and percentage on all non numeric columns'
        print '-mean' , 'computes count, mean, standard deviation, min, max on all numeric columns'
        print '-column:file <file path>', 'Provide column list from file which needs processing one column name per line'
        print '-column <inline column>', 'for small dataset provide column names inline those need processing column;column'
        print 'if no column name specified all columns in dataset will be processed'
        print '-dataset:<parquet,csv <Splitter>> <dataset file path>'
        print '-save:<parquet,csv <Splitter>> <Results file path>'
        print '-test' , 'run a test no other arguments required'
        sys.exit()
    
    
    # Validate input parameters 
    DatasetFilePath = ""
    DataSetType = ""
    DataSplitter = ","
    SaveResultFilePath = ""
    SaveResultType = ""
    SaveSplitter = ","
    ColumnModeFlag = 1
    ColumnFilePath = ""
    Columns = []
    StatsTypeFlag = False  
    StatsType = ""
    
    
    # Initiat spark and sparksql contect 
    SparkContext = ContextProvider.getSparkInstance()
    SqlContext = ContextProvider.getSQLContext()

    i = 1
    # If application is run in non test mode then parse all passed parameters and set appropriate flags and constants
    if sys.argv[1] != "-test":
        while i < len(sys.argv):
            # Parse which type of stats to compute 
            if sys.argv[i] in ['-allstats', '-freq:all', '-freq:count', '-freq:percent', '-mean']:
                if not StatsTypeFlag:
                    StatsType = sys.argv[i][1:]
                else:
                    print  'Multiple stats type specified'
                    sys.exit()
                StatsTypeFlag = True
            # Parse if a column list is passed using file 
            elif sys.argv[i] == '-column:file':
                ColumnModeFlag = 3
                i += 1
                ColumnFilePath = sys.argv[i]
            # Parse if a column list is passed inline
            elif sys.argv[i] == '-column':
                ColumnModeFlag = 2
                i += 1
                Columns = sys.argv[i].split(';')
            # Parse what type of input is provided parquet or csv
            elif sys.argv[i] == '-dataset:parquet' or sys.argv[i] == '-dataset:csv':
                DataSetType = sys.argv[i].split(':')[1]
                i += 1
                if DataSetType == 'csv':
                    DataSplitter = sys.argv[i]
                    i += 1
                DatasetFilePath = sys.argv[i]
            # Parse where to save output in parquet or in csv 
            elif sys.argv[i] == '-save:parquet' or sys.argv[i] == '-save:csv':
                SaveResultType = sys.argv[i].split(':')[1]
                i += 1
                if SaveResultType == 'csv':
                    SaveSplitter = sys.argv[i]
                    i += 1
                SaveResultFilePath = sys.argv[i]    
            i += 1
    
        # Check if minimum required parameters are passed  
        if DatasetFilePath == "" or DataSetType == "" or not StatsTypeFlag or SaveResultType == "":
            print "Invalid arguments check", sys.argv[0] , '-h for help'
            sys.exit() 
                
        # Parse if column list to be processed is passed by file
        if ColumnModeFlag == 3:
            ColumnFile = open(ColumnFilePath)
            Columns = ColumnFile.read().split('\n')
            ColumnFile.close()
        
        # Load data according to file type 
        if DataSetType == "parquet":
            DataSet = SqlContext.read.parquet(DatasetFilePath)
            SchemaFromData = False
        elif DataSetType == "csv":
            Data = SparkContext.textFile(DatasetFilePath)
            DataSet = Data.map(lambda Line: Line.split(DataSplitter)).toDF()
            SchemaFromData = True
                
            
        # Parse stats type to be processed and call appropriate module
        if StatsType == 'allstats':
            StatsTypeList = ['Frequency','Percentage'] 
            ResultDataSet = AllStats.getAllStats(DataSet, StatsTypeList , SchemaFromData, Columns)
        elif StatsType == 'freq:all':
            StatsTypeList = ['Frequency','Percentage'] 
            ResultDataSet = FreqAllStats(DataSet, StatsTypeList, SchemaFromData, Columns)
        elif StatsType == 'freq:count':
            StatsTypeList = ['Frequency']
            if Columns == []:
                ResultDataSet = FreqAllStats(DataSet, StatsTypeList, SchemaFromData)
            else:
                ResultDataSet = FreqColStats(DataSet, Columns, StatsTypeList)  
        elif StatsType == 'freq:percent':
            StatsTypeList = ['Percentage']
            if Columns == []:
                ResultDataSet = FreqAllStats(DataSet, StatsTypeList, SchemaFromData)
            else:
                ResultDataSet = FreqColStats(DataSet, Columns, StatsTypeList)
        else:
            StatsTypeList = []
            ResultDataSet = MeanAllStats(DataSet, SchemaFromData, Columns)

        Save(ResultDataSet, SaveResultFilePath, SaveResultType, SaveSplitter)
    # Run the test module 
    else: 
        RunTest() 
        