from pyspark import SparkFiles
from pyspark.sql import  SparkSession


def download_file(spark: SparkSession ,year, month, taxi_type):
  url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year:04d}-{month:02d}.csv.gz'
  spark.sparkContext.addFile(url)
  filename = SparkFiles.get(f'{taxi_type}_tripdata_{year:04d}-{month:02d}.csv.gz')
  print(f"Reading data from '{taxi_type}_tripdata_{year:04d}-{month:02d}.csv.gz")
  df = spark \
      .read \
      .csv(filename, header=True, inferSchema=True)
  return df
