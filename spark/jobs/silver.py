from utils.spark_session import get_spark_session
from utils.logger import get_logger
from spark.transform import read_parquet, merging_yellow_green_trips, split_valid_invalid
from spark.load_data import upload_to_s3
from rules.valid_condition import get_nyc_trips_condition
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--year", required=True, type=int)
parser.add_argument("--month", required=True, type=int)

args = parser.parse_args()

logger = get_logger(__name__)
if __name__ == "__main__":
  logger.info('Starting Spark sessing for silver job')
  spark = get_spark_session()

  logger.info("Reading green parquet file green_%s_%s.parquet", args.year, args.month)
  green_df = read_parquet(
    spark=spark,
    s3_url="s3a://nyc-taxi-walid-lab-2026/bronze/green/",
    taxi_type="green",
    year=args.year,
    month=args.month
  )

  logger.info("Reading yellow parquet file yellow_%s_%s.parquet", args.year, args.month)
  yellow_df = read_parquet(
    spark=spark,
    s3_url="s3a://nyc-taxi-walid-lab-2026/bronze/yellow/",
    taxi_type="yellow",
    year=args.year,
    month=args.month
  )
  logger.info("Merging yellow_df and green_df with each other")
  merged_df = merging_yellow_green_trips(yellow_trips=yellow_df, green_trips=green_df)

  logger.info("validate the results")
  good_df, bad_df = split_valid_invalid(df=merged_df, condition=get_nyc_trips_condition())

  logger.info("Uploading bad data to quarantine layer")
  upload_to_s3(
    df=bad_df,
    taxi_type="bad_nyc_trips",
    layer="quarantine"
  )
  logger.info("Uploading finished")

  logger.info("Uploading good data to silver layer")
  upload_to_s3(
    df=good_df,
    taxi_type="nyc_trips",
    layer="silver",
    partition_cols=["year", "month"]
  )
  logger.info("Uploading finished")
  spark.stop()
  logger.info("Silver job completed successfully")
