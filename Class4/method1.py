from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when
from pyspark.sql.functions import row_number, desc
import pyspark.sql.functions as sf

from datetime import datetime
import os

#EXTRACT
def etl_process(path, file_name, file_date):
    spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.executor.cores",8).getOrCreate()
    
    
    df = spark.read.json(path+file_name)
    df = df.select('_source.*')
    
    df = df.withColumn("Type",
           when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
        .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
              (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
        .when((col("AppName") == 'RELAX'), "Giải Trí")
        .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
        .when((col("AppName") == 'SPORT'), "Thể Thao")
        .otherwise("Error"))
    
    df = df.withColumn('Date', sf.lit(file_date))
    df = df.select('Contract','Type','TotalDuration', 'Date')
    df = df.filter(df.Type != 'Error')
    
    print('Finished Processing {}'.format(file_name))
    return df

#TRANSFORM
def main_task(start_date_str, end_date_str):
    start_time = datetime.now()
    path = '/Users/habaokhanh/Study_BigData_Dataset/log_content/'
    list_file = sorted([file for file in os.listdir(path) if file != '.DS_Store'])
            
    result = None
    for file_name in list_file:
        date_str = file_name.split('_')[-1].split('.')[0]
        file_date = datetime.strptime(date_str, "%Y%m%d").date()
        
        start_date = datetime.strptime(start_date_str, "%Y%m%d").date()
        end_date = datetime.strptime(end_date_str, "%Y%m%d").date()
        if start_date <= file_date <= end_date:
            df = etl_process(path, file_name, file_date)
            if result is None:
                result = df
            else:
                result = result.union(df)
                
    result_group = result.groupby('Contract','Type').sum()
    result_group = result_group.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    final = result_group.groupBy('Contract').pivot("Type").sum('TotalDuration')
    final = final.withColumnRenamed('Giải Trí', 'RelaxDuration') \
        .withColumnRenamed('Phim Truyện', 'MovieDuration') \
        .withColumnRenamed('Thiếu Nhi', 'ChildDuration') \
        .withColumnRenamed('Thể Thao', 'SportDuration') \
        .withColumnRenamed('Truyền Hình', 'TVDuration')

    #calc most_watch
    def most_watch_calc(result):
        windowSpec = Window.partitionBy("Contract").orderBy(desc("TotalDuration"))
        most_watch = result_group.withColumn("rank",row_number().over(windowSpec))
        most_watch = most_watch.filter(most_watch.rank==1)
        most_watch = most_watch.select("Contract", "Type")
        most_watch = most_watch.withColumnRenamed("Type","MostWatch")
        return most_watch
    
    #calc customer_tase
    def customer_taste_calc(final):
        final = final.withColumn("RelaxDuration",when(col("RelaxDuration").isNotNull(),"Relax").otherwise(col("RelaxDuration")))
        final = final.withColumn("MovieDuration",when(col("MovieDuration").isNotNull(),"Movie").otherwise(col("MovieDuration")))
        final = final.withColumn("ChildDuration",when(col("ChildDuration").isNotNull(),"Child").otherwise(col("ChildDuration")))
        final = final.withColumn("SportDuration",when(col("SportDuration").isNotNull(),"Sport").otherwise(col("SportDuration")))
        final = final.withColumn("TVDuration",when(col("TVDuration").isNotNull(),"TV").otherwise(col("TVDuration")))
        
        taste = final.withColumn('CustomerTaste', sf.concat_ws("-", *[i for i in final.columns if i != ['Contract']]))
        taste = taste.select('Contract','CustomerTaste')
        return taste
    
    #calc activeness
    def calculate_active_rate(result):
        active = result.groupby('Contract','Type','Date').sum()
        active = active.withColumnRenamed('sum(TotalDuration)','TotalDuration')
        active = active.groupBy('Contract','Date').pivot("Type").sum('TotalDuration')
        active = active.withColumnRenamed('Giải Trí', 'RelaxDuration') \
                        .withColumnRenamed('Phim Truyện', 'MovieDuration') \
                        .withColumnRenamed('Thiếu Nhi', 'ChildDuration') \
                        .withColumnRenamed('Thể Thao', 'SportDuration') \
                        .withColumnRenamed('Truyền Hình', 'TVDuration')

        active = active.filter((active.Date >= start_date) & (active.Date <= end_date))
        active = active.withColumn("IsActive",
                            sf.when((active.RelaxDuration > 0) | (active.MovieDuration > 0) | (active.ChildDuration > 0) | (active.SportDuration > 0) | (active.TVDuration > 0), 1)
                            .otherwise(0))
        
        # total active dates for each user
        total_days = (end_date-start_date).days+1
        activeness = active.groupBy("Contract").agg(sf.sum("IsActive").alias("ActiveDays"))

        # active rate for each user
        activeness = activeness.withColumn("ActiveRate", (sf.col("ActiveDays") / sf.lit(total_days)).cast("double"))
        
        return activeness
    
    def df_combined(final, most_watch, customer_taste, activeness):
        # 1st join with most_watch
        final_combine = final.join(most_watch, on='Contract', how='left')

        # 2nd join with customer_taste
        final_combine = final_combine.join(customer_taste, on='Contract', how='left')

        # 3rd join with activeness
        final_combine = final_combine.join(activeness, on='Contract', how='left')

        return final_combine

    #LOAD
    print('-----------Saving Data ---------')
    final.repartition(1).write.csv('/Users/habaokhanh/Study_BigData_Dataset/log_content/clean/df_clean1',header=True)
    
    end_time = datetime.now()
    time_processing = (end_time - start_time).total_seconds()
    print('It took {} to process the data'.format(time_processing))
    
    return print('Data Saved Successfully')

main_task('20220401', '20220402')
