from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import functions as F
from pyspark.ml.stat import Correlation
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os

spark = SparkSession.builder.appName("Create Logistic Regression Model").getOrCreate()

batches = [f"gs://nyc_yellow_taxi_project/transformed_nyc_yellow_taxi_data/batch_number={i}/" for i in range(12)]
data = spark.read.parquet(*batches)

assembler = VectorAssembler(
    inputCols=["VendorID", "passenger_count", "trip_distance", "fare_amount", "extra", 
               "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", 
               "total_amount", "congestion_surcharge", "airport_fee, RateCodeID"],
    outputCol="features"
)

assembled_data = assembler.transform(data)

logistic_data = assembled_data.select(col("features"), col("payment_type").alias("label"))
logistic_data.show()

assembler = VectorAssembler(
    inputCols=["VendorID", "passenger_count", "trip_distance", "fare_amount", "extra", 
               "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", 
               "total_amount", "congestion_surcharge", "airport_fee"],
    outputCol="features"
) # ,RatcodeID

assembled_data = assembler.transform(data)

logistic_data = assembled_data.select(col("features"), col("payment_type").alias("label"))
logistic_data.show()

train_data, test_data = logistic_data.randomSplit([0.8, 0.2], seed=123)

lr = LogisticRegression()
lr_model = lr.fit(train_data)

coefficient_matrix = lr_model.coefficientMatrix
intercept_vector = lr_model.interceptVector

print("Coefficient Matrix:")
print(coefficient_matrix.toArray())  

print("Intercept Vector:")
print(intercept_vector.toArray())  

predictions = lr_model.transform(test_data)
predictions.select("features", "label", "prediction").show()

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", 
    predictionCol="prediction", 
    metricName="accuracy"
)

accuracy = evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy:.2f}")
