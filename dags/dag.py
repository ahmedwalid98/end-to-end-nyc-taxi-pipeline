import subprocess

from airflow.sdk import task, dag
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from spark.extract_data import download_file
from spark.load_data import upload_to_s3
from spark.transform import read_parquet, merging_yellow_green_trips, split_valid_invalid
from utils.spark_session import get_spark_session
from utils.logger import get_logger

logger = get_logger(__name__)

#, catchup=True

@dag(
      dag_id="ingest_data_v02", schedule='@monthly', start_date=pendulum.datetime(2021, 3, 1, tz='UTC'), max_active_runs=1,tags=["ingest", "load"], catchup=True
)
def ingest_data_dag_v02():

  ingest_green = SparkSubmitOperator(
     task_id="bronze_green",
     application="/home/walid/nyc_airflow_taxi/spark/jobs/bronze.py",
     application_args= [
        "--year", "{{ data_interval_start.year }}",
        "--month", "{{ data_interval_start.month }}",
        "--taxi_type", "green"
     ],
    conn_id= "spark_default",
    packages=(
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ),
  )

  ingest_yellow = SparkSubmitOperator(
       task_id="bronze_yellow",
       application="/home/walid/nyc_airflow_taxi/spark/jobs/bronze.py",
       application_args= [
          "--year", "{{ data_interval_start.year }}",
          "--month", "{{ data_interval_start.month }}",
          "--taxi_type", "yellow"
       ],
      conn_id= "spark_default",
      packages=(
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ),
    )

  transform = SparkSubmitOperator(
      task_id="silver_task",
      application="/home/walid/nyc_airflow_taxi/spark/jobs/silver.py",
      application_args= [
          "--year", "{{ data_interval_start.year }}",
          "--month", "{{ data_interval_start.month }}"
      ],
      conn_id= "spark_default",
      packages=(
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ),  
    )
  run_crawler = GlueCrawlerRunOperator(
    task_id="run_glue_crawler",
    crawler_name="nyc-trips-crawler",
    wait_for_completion=True,
)

  @task
  def run_dbt():

    dbt_project_dir = "/home/walid/nyc_airflow_taxi/dbt_taxi_riders"

    logger.info("Running dbt project from %s", dbt_project_dir)

    subprocess.run(
        ["dbt", "run"],
        cwd=dbt_project_dir,
        check=True,
    )


  [ingest_green, ingest_yellow] >> transform
  transform >> run_crawler 
  dbt = run_dbt()
  run_crawler >> dbt
  


ingest_data_dag_v02()
  




