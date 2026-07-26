from pyspark.sql import SparkSession
def get_spark_session():
  return  SparkSession \
          .builder \
          .appName('nyc_taxi').master('local[*]') \
          .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
          .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
          .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
          .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
          .config("spark.hadoop.fs.s3a.fast.upload", "true") \
          .config("spark.hadoop.fs.s3a.path.style.access", "true") \
          .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
          .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
          .getOrCreate()

spark = get_spark_session()
print(spark.conf.get("spark.sql.shuffle.partitions"))