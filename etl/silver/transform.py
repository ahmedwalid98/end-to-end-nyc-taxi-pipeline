from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

AWS_BUCKET='nyc-taxi-walid-lab-2026'


def read_parquet(spark: SparkSession, s3_url, taxi_type, year, month) -> DataFrame:
  print(f'Reading {s3_url} data')
  df: DataFrame = spark.read.parquet(s3_url)
  filtered_df = df.where(
  (col('passenger_count') > 0) & 
  (col('trip_distance') > 0) &
  (col('fare_amount') > 0) &
  (col('total_amount') > 0)
         )
  if taxi_type == 'green':
    renamed_df = filtered_df \
      .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') \
      .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
      .withColumn('taxi_type', lit(taxi_type))

  if taxi_type == 'yellow':
      renamed_df = filtered_df \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumn('trip_type', lit(0)) \
        .withColumn('ehail_fee', lit(0)) \
        .withColumn('taxi_type', lit(taxi_type))

  final_df = renamed_df \
    .withColumn('year', lit(year)) \
    .withColumn('month', lit(month))
  
  return final_df 

def merging_yellow_green_trips(yellow_trips: DataFrame, green_trips: DataFrame) -> DataFrame:
   print('Merging two datasets')
   merged_df = yellow_trips.unionByName(green_trips)
   return merged_df


