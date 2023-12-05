from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofmonth, month, year, avg, count, desc, weekofyear, current_date, row_number, expr
from pyspark.sql.window import Window

import zipfile
import pandas as pd
from io import BytesIO
import os

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import to_timestamp

def read_data(spark, files):
    dfs = []

    # Схема для першого DataFrame
    schema_1 = StructType([
        StructField("ride_id", StringType(), True),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("ended_at", TimestampType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", LongType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", DoubleType(), True),
        StructField("start_lat", DoubleType(), True),
        StructField("start_lng", DoubleType(), True),
        StructField("end_lat", DoubleType(), True),
        StructField("end_lng", DoubleType(), True),
        StructField("member_casual", StringType(), True),
    ])

    # Схема для другого DataFrame
    schema_2 = StructType([
        StructField("trip_id", LongType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("bikeid", LongType(), True),
        StructField("tripduration", StringType(), True),
        StructField("from_station_id", LongType(), True),
        StructField("from_station_name", StringType(), True),
        StructField("to_station_id", LongType(), True),
        StructField("to_station_name", StringType(), True),
        StructField("usertype", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birthyear", DoubleType(), True),
    ])

    for zip_path, csv_path in files:
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open(csv_path) as f:
                zip_data = BytesIO(f.read())
                print(zip_data)

                if 'Divvy_Trips_2020_Q1' in zip_path:
                    df_pandas = pd.read_csv(zip_data)
                    df_pandas['started_at'] = pd.to_datetime(df_pandas['started_at'])
                    df_pandas['ended_at'] = pd.to_datetime(df_pandas['ended_at'])
                    df_spark = spark.createDataFrame(df_pandas, schema=schema_1)
                elif 'Divvy_Trips_2019_Q4' in zip_path:
                    df_pandas = pd.read_csv(zip_data)
                    df_pandas['start_time'] = pd.to_datetime(df_pandas['start_time'])
                    df_pandas['end_time'] = pd.to_datetime(df_pandas['end_time'])
                    df_spark = spark.createDataFrame(df_pandas, schema=schema_2)
                else:
                    pass

                dfs.append(df_spark)

    return dfs[0], dfs[1]

def calculate_average_trip_duration(df_spark_1, df_spark_2):
    df_spark_1 = df_spark_1.withColumn("tripduration", expr("unix_timestamp(ended_at) - unix_timestamp(started_at)"))
    df_spark_1 = df_spark_1.withColumn("date", to_date(col("started_at")))

    df_spark_2 = df_spark_2.withColumn("tripduration", expr("unix_timestamp(end_time) - unix_timestamp(start_time)"))
    df_spark_2 = df_spark_2.withColumn("date", to_date(col("start_time")))

    result_1 = df_spark_1.groupBy("date").agg(avg("tripduration").alias("average_trip_duration"))
    result_2 = df_spark_2.groupBy("date").agg(avg("tripduration").alias("average_trip_duration"))

    result_1.repartition(1).write.csv("reports/average_trip_duration_per_day_1", mode='overwrite', header=True)
    result_2.repartition(1).write.csv("reports/average_trip_duration_per_day_2", mode='overwrite', header=True)


def count_trips_per_day(df_spark_1, df_spark_2):
    df_spark_1 = df_spark_1.withColumn("date", to_date(col("started_at")))
    df_spark_2 = df_spark_2.withColumn("date", to_date(col("start_time")))

    result_1 = df_spark_1.groupBy("date").agg(count("ride_id").alias("trips_per_day"))
    result_2 = df_spark_2.groupBy("date").agg(count("trip_id").alias("trips_per_day"))

    result_1.repartition(1).write.csv("reports/trips_per_day_1.csv", mode='overwrite', header=True)
    result_2.repartition(1).write.csv("reports/trips_per_day_2.csv", mode='overwrite', header=True)


def popular_start_stations_per_month(df_spark_1, df_spark_2):
    df_spark_1 = df_spark_1.withColumn("month", month(col("started_at")))
    df_spark_2 = df_spark_2.withColumn("month", month(col("start_time")))

    windowSpec_1 = Window.partitionBy("month").orderBy(desc("count"))
    result_1 = df_spark_1.groupBy("month", "start_station_name").agg(count("ride_id").alias("count"))
    result_1 = result_1.withColumn("rank", row_number().over(windowSpec_1)).filter(col("rank") == 1)

    windowSpec_2 = Window.partitionBy("month").orderBy(desc("count"))
    result_2 = df_spark_2.groupBy("month", "from_station_name").agg(count("trip_id").alias("count"))
    result_2 = result_2.withColumn("rank", row_number().over(windowSpec_2)).filter(col("rank") == 1)

    result_1.repartition(1).write.csv("reports/popular_start_stations_per_month_1", mode='overwrite', header=True)
    result_2.repartition(1).write.csv("reports/popular_start_stations_per_month_2", mode='overwrite', header=True)

def top_stations_last_two_weeks(df_spark_1, df_spark_2):
    df_spark_1 = df_spark_1.withColumn("week", weekofyear(col("started_at")))
    df_spark_2 = df_spark_2.withColumn("week", weekofyear(col("start_time")))

    windowSpec_1 = Window.partitionBy("week").orderBy(desc("count"))
    result_1 = df_spark_1.groupBy("week", "start_station_name").agg(count("ride_id").alias("count"))
    result_1 = result_1.withColumn("rank", row_number().over(windowSpec_1)).filter(col("rank").between(1, 3))

    windowSpec_2 = Window.partitionBy("week").orderBy(desc("count"))
    result_2 = df_spark_2.groupBy("week", "from_station_name").agg(count("trip_id").alias("count"))
    result_2 = result_2.withColumn("rank", row_number().over(windowSpec_2)).filter(col("rank").between(1, 3))

    result_1.repartition(1).write.csv("reports/top_stations_last_two_weeks_1", mode='overwrite', header=True)
    result_2.repartition(1).write.csv("reports/top_stations_last_two_weeks_2", mode='overwrite', header=True)

def gender_comparison(df_spark_1, df_spark_2):
    result_2 = df_spark_2.groupBy("gender").agg(avg("tripduration").alias("average_trip_duration"))

    result_2.repartition(1).write.csv("reports/gender_comparison_2", mode='overwrite', header=True)

def age_analysis(df_spark_1, df_spark_2):
    df_spark_2 = df_spark_2.withColumn("age", year(current_date()) - col("birthyear"))

    result_2 = df_spark_2.groupBy("age").agg(avg("tripduration").alias("average_trip_duration"))

    result_longest_2 = result_2.orderBy(desc("average_trip_duration")).limit(10)
    result_shortest_2 = result_2.orderBy("average_trip_duration").limit(10)

    result_longest_2.repartition(1).write.csv("reports/top_10_longest_trips_by_age_2", mode='overwrite', header=True)
    result_shortest_2.repartition(1).write.csv("reports/top_10_shortest_trips_by_age_2", mode='overwrite', header=True)

def main():
    spark = SparkSession.builder.appName("Exercise6").getOrCreate()

    files = [
        ('data/Divvy_Trips_2020_Q1.zip', 'Divvy_Trips_2020_Q1.csv'),
        ('data/Divvy_Trips_2019_Q4.zip', 'Divvy_Trips_2019_Q4.csv')
    ]

    df_spark_1, df_spark_2 = read_data(spark, files)

    # Задачі
    calculate_average_trip_duration(df_spark_1, df_spark_2)
    count_trips_per_day(df_spark_1, df_spark_2)
    popular_start_stations_per_month(df_spark_1, df_spark_2)
    top_stations_last_two_weeks(df_spark_1, df_spark_2)
    gender_comparison(df_spark_1, df_spark_2)
    age_analysis(df_spark_1, df_spark_2)

    spark.stop()

if __name__ == "__main__":
    main()
