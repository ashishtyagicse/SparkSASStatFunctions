# SparkSASStatFunctions
This project is aimed at making SAS Proc freq and Proc Mean type functionality available in Spark 

SAS has many useful stats function and one of them is Proc Freq and Proc Mean these are some of the most commonly used functions.

In many cases users just wants to see these high-level stats for each column in their data. 

While SAS is excellent in providing these and other stats, when data grows beyond a certain size processing such huge amount of data becomes a bottleneck.

Spark is just the right tool for processing such huge amounts of data but it does not have a standard function to compute these stats on tables like SAS.

This utility aims at filling that gap using sparks processing power.

### Utility Structure 
- SASFunctions Package
  This package contains all modules that mimic corresponding SAS functions
  - Freq Module: 
  
    This module calculates the count as frequency and percentage for each group of values in a string based column in given dataset.
    
    Module has two main functions 
    - getAllStats: This function takes a data frame as input and if schema is not present in dataframe then it infers it automatically if column is string type or not. 
    It internally calls the getColumnStats function with a list of columns for which stats need to be computed.
    Optionally you can also provide a list of columns in which case only those columns will be processed.
    - getColumnStats: This function calls sub functions to calculate frequency and percentage and then combine and reformats the data to match rest of the application.
  - Mean Module:
   
    This module calculates count, mean, standard deviation, min value and max value for ant numeric column in the dataset provided.
      
    Same as Freq module this module has two main functions 
    - getAllStats: This function takes a data frame as input and if schema is not present in dataframe then it infers it automatically if column is numeric type or not. 
    It internally calls the getColumnStats function with a list of columns for which stats need to be computed.
    Optionally you can also provide a list of columns in which case only those columns will be processed.
    - getColumnStats: This function calls sub functions to calculate mean related stats and then combine and reformats the data to match rest of the application.
  - AllStats:
  
    This module is a wrapper module which internally calls Freq and Mean module functions and combine their outputs in one dataframe for consistency.
    
  - Schema:
  
    This module checks each value in a column to determine if a column is string type or numeric type or decimal type.
    Inferring schema from data in this manner can slow down processing so it is advisable to use parquet files which already contains the schema information.
    
- SparakStats:
  
  This is the main entry point for utility and provides various command line arguments to specify the input and output data types and storage locations and what type of stats needs to be computed.
  
  use `spark-submit SparkStats.py -h` for full list of options
  
- TestModules:
  
  To test the functionality and see output formats utility includes a test module which can be run by submitting 
  
  `spark-submit SparkStats.py -test`
  
  This test utility uses files in TestData folder so make sure all those files are available while running the utility in test mode.
  
  
### Run Commands:
  
  Following are a few examples to run the utility 
  - Compute all stats on a parquet file
  
    ```spark-submit SparkStats.py -allstats -dataset:parquet example.parquet -save:parquet output.parquet```
  
  - Compute all frequency stats on a parquet file
  
    ```spark-submit SparkStats.py -freq:all -dataset:parquet example.parquet -save:parquet output.parquet```

  - Compute all mean stats on a csv file delimited by ';' and save output in another csv file delimited by '|'
  
    ```spark-submit SparkStats.py -mean -dataset:csv ';' example.csv -save:csv '|' output.csv```
  
  - Compute all mean stats on parquet file but only process two columns 
  
    ```spark-submit SparkStats.py -mean -column "State;Revenue" -dataset:parquet example.parquet -save:parquet output.parquet```
  
  - Compute all mean stats on parquet file but only process two columns provided in a file
  
    ```spark-submit SparkStats.py -mean -column:file "columns.txt" -dataset:parquet example.parquet -save:parquet output.parquet```
