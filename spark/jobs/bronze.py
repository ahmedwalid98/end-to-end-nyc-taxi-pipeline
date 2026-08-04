from utils.spark_session import get_spark_session
from utils.logger import get_logger
from spark.extract_data import download_file, read_csv
from spark.load_data import upload_to_s3
import argparse
logger = get_logger(__name__)
parser = argparse.ArgumentParser()
parser.add_argument("--year", type=int)
parser.add_argument("--month", type=int)
parser.add_argument("--taxi_type")

args = parser.parse_args()

import os
import socket


if __name__ == "__main__":
  print("HOSTNAME:", socket.gethostname())
  print("PWD:", os.getcwd())
  print("PATH EXISTS:", os.path.exists("/opt/spark/work-dir/data"))
  logger.info('Started spark session')
  spark = get_spark_session()
  logger.info('Downloading file %s_tripdata_%s_%s.csv', args.taxi_type, args.year, args.month)
  path = download_file(
    year=args.year,
    month=args.month,
    taxi_type=args.taxi_type
  )
  df = read_csv(
    spark=spark,
    path=path
  )
  logger.info('Download done')
  logger.info('Uploading file %s_tripdata_%s_%s.parquet to bronze layer', args.taxi_type, args.year, args.month)

  upload_to_s3(
    df=df,
    taxi_type=args.taxi_type,
    layer='bronze'
  )


  logger.info("Bronze upload completed")

  spark.stop()

  logger.info("Spark session stopped")

