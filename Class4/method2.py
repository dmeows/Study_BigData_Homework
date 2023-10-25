from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from datetime import datetime
import os

def etl_process(path, file_name):
    spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.executor.cores", 8).getOrCreate()

    df = spark.read.json(path + file_name)
    df = df.select('_source.*')

    df = df.withColumn("Type",
                       when((col("AppName") == 'CHANNEL') | (col("AppName") == 'DSHD') | (col("AppName") == 'KPLUS') | (col("AppName") == 'KPlus'), "Truyền Hình")
                       .when((col("AppName") == 'VOD') | (col("AppName") == 'FIMS_RES') | (col("AppName") == 'BHD_RES') |
                             (col("AppName") == 'VOD_RES') | (col("AppName") == 'FIMS') | (col("AppName") == 'BHD') | (
                                     col("AppName") == 'DANET'), "Phim Truyện")
                       .when((col("AppName") == 'RELAX'), "Giải Trí")
                       .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
                       .when((col("AppName") == 'SPORT'), "Thể Thao")
                       .otherwise("Error"))

    df = df.select('Contract', 'Type', 'TotalDuration')
    df = df.filter(df.Type != 'Error')

    #Process one df at a time
    result = df.groupBy('Contract', 'Type').sum()
    result = result.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')
    result = df.groupBy("Contract").pivot("Type").sum("TotalDuration")

    print('Finished Processing {}'.format(file_name))
    return result


def main_task(start_date_str, end_date_str):
    start_time = datetime.now()
    path = "/Users/habaokhanh/Study_BigData_Dataset/log_content/"
    list_file = [file for file in os.listdir(path) if file != '.DS_Store']
    
    result = None
    for file_name in list_file:
        date_str = file_name.split('_')[-1].split('.')[0]
        file_date = datetime.strptime(date_str, "%Y%m%d").date()
        
        start_date = datetime.strptime(start_date_str, "%Y%m%d").date()
        end_date = datetime.strptime(end_date_str, "%Y%m%d").date()

        if start_date <= file_date <= end_date:
            df = etl_process(path, file_name)
            if result is None:
                result = df
            else:
                result = result.union(df)

    result = result.groupby('Contract').sum()
    result = result.withColumnRenamed('sum(Giải Trí)', 'RelaxDuration') \
        .withColumnRenamed('sum(Phim Truyện)', 'MovieDuration') \
        .withColumnRenamed('sum(Thiếu Nhi)', 'ChildDuration') \
        .withColumnRenamed('sum(Thể Thao)', 'SportDuration') \
        .withColumnRenamed('sum(Truyền Hình)', 'TVDuration')

    print('-----------Saving Data ---------')
    result.repartition(1).write.csv('/Users/habaokhanh/Study_BigData_Dataset/log_content/df_clean2', header=True)
    
    end_time = datetime.now()
    time_processing = (end_time - start_time).total_seconds()
    print('It took {} to process the data'.format(time_processing))
    return print('Data Saved Successfully')


main_task('20220401', '20220402')
