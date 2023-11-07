from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, desc
from pyspark.sql.functions import row_number
import pyspark.sql.functions as sf
from datetime import datetime
import os

# EXTRACT
def get_spark_session():
    return SparkSession.builder.config("spark.driver.memory", "8g").config(
        "spark.executor.cores", 8).getOrCreate()


def read_json_data(spark, path, file_name):
    data = spark.read.json(path + file_name)
    return data.select('_source.*')


def add_type_column(df):
    tv_condition_list = ['CHANNEL', 'DSHD', 'KPLUS', 'KPlus']
    movie_condition_list = ['VOD', 'FIMS_RES', 'BHD_RES', 'VOD_RES', 'FIMS', 'BHD', 'DANET']
    df = df.withColumn("Type",
                       when(col("AppName").isin(*tv_condition_list), "Truyền Hình")
                       .when(col("AppName").isin(*movie_condition_list),"Phim Truyện")
                       .when(col("AppName") == 'RELAX', "Giải Trí")
                       .when(col("AppName") == 'CHILD', "Thiếu Nhi")
                       .when(col("AppName") == 'SPORT', "Thể Thao")
                       .otherwise("Error"))
    return df


def add_date_filter_non_error(df, file_date):
    df = df.withColumn('Date', sf.lit(file_date))
    return df.filter(df.Type != 'Error').select('Contract', 'Type', 'TotalDuration', 'Date')

# TRANSFORM
def calculate_most_watched(df):
    windowSpec = Window.partitionBy("Contract").orderBy(desc("TotalDuration"))
    most_watched = df.withColumn("rank", row_number().over(windowSpec))
    most_watched = most_watched.filter(most_watched.rank == 1)
    return most_watched.select("Contract", "Type").withColumnRenamed("Type", "MostWatch")


def calculate_customer_taste(df):
    for column in df.columns:
        if 'Duration' in column:
            df = df.withColumn(column, when(col(column).isNotNull(), column.replace("Duration", "")))
    customer_taste = df.withColumn('CustomerTaste',
                                   sf.concat_ws("-", *[i for i in df.columns if 'Duration' in i]))
    customer_taste = customer_taste.select('Contract', 'CustomerTaste')
    return customer_taste


def calculate_activeness(df, start_date, end_date):
    total_days = (end_date - start_date).days + 1
    
    active = df.groupby('Contract', 'Date').agg((sf.sum('TotalDuration').alias('TotalDurationPerDay')))
    active = active.withColumn("IsActive", sf.when(active.TotalDurationPerDay > 0, 1).otherwise(0))

    activeness = active.groupBy("Contract").agg(sf.sum("IsActive").alias("ActiveDays"))
    activeness = activeness.withColumn("ActiveRate", (sf.col("ActiveDays") / sf.lit(total_days)))
    return activeness


def join_dataframes(final_df, most_watched_df, customer_taste_df, activeness_df):
    final_df = final_df.join(most_watched_df, on='Contract', how='left')
    final_df = final_df.join(customer_taste_df, on='Contract', how='left')
    final_df = final_df.join(activeness_df, on='Contract', how='left')
    return final_df


def write_data(df):
    df.repartition(1).write.csv('/Users/habaokhanh/Study_BigData_Dataset/log_content/clean/df_clean1', header=True)

# MAIN FUNCTION
def main_task(start_date_str, end_date_str):
    start_time = datetime.now()

    # Extract
    path = '/Users/habaokhanh/Study_BigData_Dataset/log_content/'
    list_file = sorted([file for file in os.listdir(path) if file != '.DS_Store'])
    spark = get_spark_session()
    start_date = datetime.strptime(start_date_str, "%Y%m%d").date()
    end_date = datetime.strptime(end_date_str, "%Y%m%d").date()

    result_df = None
    for file_name in list_file:
        file_date_str = file_name.split('_')[-1].split('.')[0]
        file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
        if start_date <= file_date <= end_date:
            df = read_json_data(spark, path, file_name)
            df = add_type_column(df)
            df = add_date_filter_non_error(df, file_date)

            if result_df is None:
                result_df = df
            else:
                result_df = result_df.union(df)

    # Transform
    most_watched_df = calculate_most_watched(result_df)
    customer_taste_df = calculate_customer_taste(result_df)
    activeness_df = calculate_activeness(result_df, start_date, end_date)

    # Join dataframes
    final_df = join_dataframes(result_df, most_watched_df, customer_taste_df, activeness_df)
    # Print final dataframe
    final_df.show()

    # Load
    write_data(final_df)

    end_time = datetime.now()
    print(f'It took {(end_time - start_time).total_seconds()} to process the data')

# RUN MAIN FUNCTION
main_task('20220401', '20220402')
