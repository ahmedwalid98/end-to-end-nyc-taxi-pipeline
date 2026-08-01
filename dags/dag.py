from airflow.sdk import task, dag
import pendulum
from etl.extract_data import download_file
from etl.load_data import upload_to_s3
from etl.silver.transform import read_parquet, merging_yellow_green_trips
from utils.spark_session import get_spark_session

#, catchup=True

@dag(
      dag_id="ingest_data_v03", schedule='@monthly', start_date=pendulum.datetime(2021, 3, 1, tz='UTC'), max_active_runs=1,tags=["ingest", "load"], catchup=True
)
def ingest_data_dag_v03():

  
  @task()
  def ingest_and_upload(taxi_type, data_interval_start=None):
      
      spark = get_spark_session()
      year = data_interval_start.year # type: ignore
      month = data_interval_start.month # type: ignore
      df = download_file(spark=spark,year=year, month=month, taxi_type=taxi_type)
      url = upload_to_s3(df= df,taxi_type=taxi_type, layer='bronze')
      return {"data_url": url, "taxi_type": taxi_type}

  @task()
  def tansform_data(content, data_interval_start=None):
    spark = get_spark_session()
    green_url = next(c['data_url'] for c in content if c['taxi_type'] == 'green')
    yellow_url = next(c['data_url'] for c in content if c['taxi_type'] == 'yellow')
    year = data_interval_start.year # type: ignore
    month = data_interval_start.month # type: ignore
    green_df = read_parquet(spark=spark, s3_url=green_url, taxi_type='green', year=year, month=month)
    yellow_df = read_parquet(spark=spark, s3_url=yellow_url, taxi_type='yellow', year=year, month=month)

    merged_df = merging_yellow_green_trips(yellow_trips=yellow_df, green_trips=green_df)
    upload_to_s3(df=merged_df, taxi_type='nyc_trips',layer='silver', partition_cols=['year', 'month'])




  taxi_types = ['green', 'yellow']
  ingest = ingest_and_upload.expand(taxi_type=taxi_types)
  tansform_data(ingest)

  


ingest_data_dag_v03()
  




