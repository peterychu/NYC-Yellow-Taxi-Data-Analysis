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
    
    fill_avg_values = df.groupBy("PULocationID", "DOLocationID").agg(
    F.avg("trip_distance").alias("avg_trip_distance"),
    F.avg("fare_amount").alias("avg_fare_amount"),
    F.avg("tip_amount").alias("avg_tip_amount"),
    F.avg("total_amount").alias("avg_total_amount"),
    F.avg("passenger_count").alias("avg_passenger_count"),
    F.avg("congestion_surcharge").alias("avg_congestion_surcharge"),
    F.avg("airport_fee").alias("avg_airport_fee")
    
    )

    data_with_avg_values =  df.join(fill_avg_values, on=["PULocationID", "DOLocationID"], how="left")

    filled_data = data_with_avg_values.withColumn(
        "trip_distance", F.coalesce("trip_distance", "avg_trip_distance")
    ).withColumn(
        "fare_amount", F.coalesce("fare_amount", "avg_fare_amount")
    ).withColumn(
        "tip_amount", F.coalesce("tip_amount", "avg_tip_amount")
    ).withColumn(
        "total_amount", F.coalesce("total_amount", "avg_total_amount")
    ).withColumn(
        "passenger_count", F.coalesce("passenger_count", "avg_passenger_count")
    ).withColumn(
        "congestion_surcharge", F.coalesce("congestion_surcharge", "avg_congestion_surcharge")
    ).withColumn(
        "airport_fee", F.coalesce("airport_fee", "avg_airport_fee")
    )

    filled_data = filled_data.drop("avg_trip_distance", "avg_fare_amount", "avg_tip_amount", "avg_total_amount", "avg_congestion_surcharge", "avg_passenger_count", "avg_airport_fee")

    filled_data = filled_data.fillna(7,subset=['RateCodeID'])
    
    avg_trip_distance = df.agg(F.avg("trip_distance")).first()[0]
    avg_fare_amount = df.agg(F.avg("fare_amount")).first()[0]
    avg_tip_amount = df.agg(F.avg("tip_amount")).first()[0]
    avg_total_amount = df.agg(F.avg("total_amount")).first()[0]
    avg_passenger_count = df.agg(F.avg("passenger_count")).first()[0]
    avg_congestion_surcharge = df.agg(F.avg("congestion_surcharge")).first()[0]
    avg_airport_fee = df.agg(F.avg("airport_fee")).first()[0]

    filled_data = filled_data.fillna({
        "trip_distance": avg_trip_distance,
        "fare_amount": avg_fare_amount,
        "tip_amount": avg_tip_amount,
        "total_amount": avg_total_amount,
        "passenger_count": avg_passenger_count,
        "congestion_surcharge": avg_congestion_surcharge,
        "airport_fee": avg_airport_fee
    })

     
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