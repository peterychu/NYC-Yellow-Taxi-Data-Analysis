from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from google.cloud import storage
from pyspark.sql.functions import col, year
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("TransformAndLoadData") \
    .getOrCreate()

source_bucket = "nyc_yellow_taxi_project"
source_prefix = "raw_nyc_yellow_taxi_data/"
destination_bucket = "nyc_yellow_taxi_project"
destination_prefix = "transformed_nyc_yellow_taxi_data/"

def list_files(bucket_name, prefix=""):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    files = [blob.name[len(prefix):] for blob in bucket.list_blobs(prefix=prefix) if blob.name.endswith('.parquet')]
    # -15-11 -10-8
    #sorted_files = sorted(files, key=lambda x: (int(x[-15:-11]), int(x[-10:-8])))
    sorted_files = sorted(files, key=lambda x: (int(x[-15:-11]), int(x[-10:-8])))
    return sorted_files
    
files = list_files(source_bucket,source_prefix)

concatenated_df = None
batch_number = 0
global_index = 0
for file_path in files:
    df = spark.read.parquet(f"gs://{source_bucket}/{source_prefix}/{file_path}")

    if 'Airport_fee' in df.columns:
          df = df.withColumnRenamed('Airport_fee', 'airport_fee')
        
    df = df.withColumn("year", year(df["tpep_pickup_datetime"]))
    df = df.filter((col("year") == 2022) | (col("year") == 2023))
    df = df.drop('year')
    df = df.withColumn('batch_number', lit(batch_number))
     
    windowSpec = Window.orderBy(F.monotonically_increasing_id())
    df = df.withColumn("index", F.row_number().over(windowSpec) - 1 + global_index)
    global_index += df.count()
    casted_df = (df
             .withColumn('VendorID', col('vendorID').cast(IntegerType()))
             .withColumn('passenger_count', col('passenger_count').cast(IntegerType()))
             .withColumn('trip_distance', col('trip_distance').cast(FloatType()))
             .withColumn('RatecodeID', col('RatecodeID').cast(IntegerType())))
             
    casted_df.write.mode('append').partitionBy("batch_number").parquet(f"gs://{destination_bucket}/{destination_prefix}/")
        
    batch_number += 1

spark.stop()
    
