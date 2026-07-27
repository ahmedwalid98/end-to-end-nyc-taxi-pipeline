from pyspark.sql import DataFrame
AWS_BUCKET='nyc-taxi-walid-lab-2026'


def upload_to_s3(df: DataFrame ,taxi_type, layer, partition_cols: list | None = None):
  print('Uploading files to S3')
  url = f"s3a://{AWS_BUCKET}/{layer}/{taxi_type}/"
  if partition_cols is not None:
    df.repartition(4).write \
    .mode("append") \
    .partitionBy(partition_cols) \
    .parquet(f"s3a://{AWS_BUCKET}/{layer}/{taxi_type}/")
  else:
    df.write \
      .mode("append") \
      .parquet(f"s3a://{AWS_BUCKET}/{layer}/{taxi_type}/")
  print('files uploaded to S3')
  return url
   
