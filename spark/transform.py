from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from utils.logger import get_logger

logger = get_logger(__name__)

AWS_BUCKET='nyc-taxi-walid-lab-2026'


def read_parquet(spark: SparkSession, s3_url, taxi_type, year, month) -> DataFrame:
  logger.info(f'Reading {s3_url} data')
  df: DataFrame = spark.read.parquet(s3_url)
  if taxi_type == 'green':
    renamed_df = df \
      .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') \
      .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
      .withColumn('taxi_type', lit(taxi_type))

  if taxi_type == 'yellow':
      renamed_df = df \
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
   logger.info("Merging yellow and green trips")
   merged_df = yellow_trips.unionByName(green_trips)
   logger.info("Successfully merged yellow and green trips")
   return merged_df

def split_valid_invalid(df: DataFrame, condition) -> tuple[DataFrame, DataFrame]:
   good_df = df.filter(condition)
   bad_df = df.filter(~condition)
   return good_df, bad_df


