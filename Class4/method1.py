#%% import libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
import pyspark.sql.functions as sf
import os
from datetime import datetime

#%% set up env
spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.executor.cores",8).getOrCreate()

#%% Read & map data in 1 day
def etl_1_day(path ,file_name):
    #print('---------------------')
    #print('Read data from Kafka')
    #print('---------------------')
    df = spark.read.json(path+file_name)
    #print('---------------------')
    #print('Showing top 5')
    #print('---------------------')
    #df.show()
    #print('---------------------')
    #print('Showing data structure')
    #print('---------------------')
    #df.printSchema()
    #print('---------------------')
    #print('Showing data value')
    #print('---------------------')
    df = df.select('_source.*')
    # df.show()
    # print('---------------------')
    # print('ETL Start')
    # print('---------------------')
    df = df.withColumn("Type",
           when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
        .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
              (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
        .when((col("AppName") == 'RELAX'), "Giải Trí")
        .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
        .when((col("AppName") == 'SPORT'), "Thể Thao")
        .otherwise("Error"))
    # print('---------------------')
    # print('Showing new data structure')
    # print('---------------------')
    df = df.select('Contract','Type','TotalDuration')
    df = df.filter(df.Type != 'Error')
    # df.printSchema()
    # df.select('Contract','Type','TotalDuration').show(5)
    # df = df.groupBy('Contract','Type').sum()
    # df = df.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    # df = df.cache()
    # print('---------------------')
    # print('Pivoting Data')
    # print('---------------------')
    # result = df.groupBy("Contract").pivot("Type").sum("TotalDuration")
    # print('---------------------')
    # print('Showing result')
    # print('---------------------')
    # result.show(5)
    print('Finished Processing {}'.format(file_name))
    return df

#%% ETL task
def main_task():
    start_time = datetime.now()
    path = '/Users/habaokhanh/Study_BigData_Dataset/log_content/'
    list_file = os.listdir(path)
    file_name = list_file[0]
    result1 = etl_1_day(path ,file_name)
    for i in list_file[1:]:
        file_name2 = i 
        result2 = etl_1_day(path ,file_name2)
        result1 = result1.union(result2)
        result1 = result1.cache()
    result1 = result1.groupby('Contract','Type').sum()
    result1 = result1.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    final = result1.groupBy("Contract").pivot("Type").sum("TotalDuration")
    print('-----------Saving Data ---------')
    final.repartition(1).write.csv('/Users/habaokhanh/Study_BigData_Dataset/log_content/df_clean1',header=True)
    end_time = datetime.now()
    time_processing = (end_time - start_time).total_seconds()
    print('It took {} to process the data'.format(time_processing))
    return print('Data Saved Successfully')

main_task()