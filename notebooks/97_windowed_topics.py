# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

eventhub_namespace = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_namespace")
eventhub_enriched = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_enriched")
eventhub_key = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_key")
readConnectionString = "Endpoint=sb://{ns}.servicebus.windows.net/;" \
                       "EntityPath={name};SharedAccessKeyName=RootManageSharedAccessKey;" \
                       "SharedAccessKey={key}".format(ns=eventhub_namespace, name=eventhub_enriched, key=eventhub_key)
ehReadConf = {
  'eventhubs.connectionString': readConnectionString
}

# COMMAND ----------

inputStream = spark.readStream \
  .format("eventhubs") \
  .options(**ehReadConf) \
  .load()
  
# Cast the data as string (it comes in as binary by default)
bodyNoSchema = inputStream.selectExpr("CAST(body as STRING)", "CAST (enqueuedTime as timestamp) AS Time")

# COMMAND ----------

# Define the schema to apply to the data...
schema = StructType([
  StructField("eventId", StringType()),
  StructField("dataItemId", StringType()),
  StructField("data", StructType([
    StructField("sentiment", IntegerType()),
    StructField("category", StringType()),
    StructField("location",StructType([
      StructField("name", StringType()),
      StructField("entity", StringType())
    ])),
  ])),
  StructField("eventName", StringType()),
  StructField("eventTopic", StringType())
])

# Apply the schema...
bodyWithSchema = bodyNoSchema.select("body", from_json(col("body"), schema).alias("data"), col("Time"))

# Filter down to just the data that we're looking for...
# Watermarking to account for late arriving events
challenge3 = bodyWithSchema \
  .withColumn("eventId", col("data.eventId")) \
  .withColumn("dataItemId", col("data.dataItemId")) \
  .withColumn("eventName", col("data.eventName")) \
  .withColumn("eventTopic", col("data.eventTopic")) \
  .withColumn("category", col("data.data.category")) \
  .withColumn("sentiment", col("data.data.sentiment")) \
  .withColumn("location", col("data.data.location.name")) \
  .withColumn("location_entity", col("data.data.location.entity")) \
  .withColumn("eventTime", col("Time").cast(TimestampType())) \
  .withWatermark("Time", "10 minute") \

challenge3.createOrReplaceTempView("ChallengeData")

