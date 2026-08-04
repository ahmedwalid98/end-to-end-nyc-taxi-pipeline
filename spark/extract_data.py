from pyspark.sql import  SparkSession
from utils.logger import get_logger
import requests
import os

logger = get_logger(__name__)


def download_file( year, month, taxi_type):
  url = (
      f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
      f"{taxi_type}/{taxi_type}_tripdata_{year:04d}-{month:02d}.csv.gz"
  )

  logger.info("Downloading %s", url)
  container_dir = "/opt/spark/work-dir/data"

  os.makedirs(container_dir, exist_ok=True)
  filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.csv.gz"
  container_path = os.path.join(container_dir, filename)


  response = requests.get(url)
  response.raise_for_status()
  with open(container_path, "wb") as f:
      f.write(response.content)

  logger.info(f"✅ Downloaded to {container_path}")
  return container_path

def read_csv(spark: SparkSession, path):
  print(F"IS {path} exists: {os.path.exists(path)}")

  df = (
      spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  )

  logger.info("Finished reading dataset")

  return df