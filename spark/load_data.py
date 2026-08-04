from pyspark.sql import DataFrame
from utils.logger import get_logger
AWS_BUCKET='nyc-taxi-walid-lab-2026'
logger = get_logger(__name__)

def upload_to_s3(df: DataFrame ,taxi_type, layer, partition_cols: list | None = None):
  logger.info(
        "Uploading %s to %s layer",
        taxi_type,
        layer
    )
  url = f"s3a://{AWS_BUCKET}/{layer}/{taxi_type}/"
  if partition_cols is not None:
    df.repartition(64).write \
    .mode("append") \
    .partitionBy(partition_cols) \
    .parquet(f"s3a://{AWS_BUCKET}/{layer}/{taxi_type}/")
  else:
    df.write \
      .mode("append") \
      .parquet(f"s3a://{AWS_BUCKET}/{layer}/{taxi_type}/")
    logger.info("Upload completed.")
    return url
   
