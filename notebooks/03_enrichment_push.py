# Databricks notebook source
import requests
from pyspark.sql.functions import col, from_json, to_json, udf, struct
from pyspark.sql.types import ArrayType, BooleanType, FloatType, StringType, StructType, DateType, IntegerType

# Set up the Event Hub config dictionary with default settings
eventhub_namespace = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_namespace")
eventhub_input = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_input")
eventhub_enriched = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_enriched")
eventhub_key = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_key")
readConnectionString = "Endpoint=sb://{ns}.servicebus.windows.net/;" \
                       "EntityPath={name};SharedAccessKeyName=RootManageSharedAccessKey;" \
                       "SharedAccessKey={key}".format(ns=eventhub_namespace, name=eventhub_input, key=eventhub_key)
writeConnectionString ="Endpoint=sb://{ns}.servicebus.windows.net/;" \
                       "EntityPath={name};SharedAccessKeyName=RootManageSharedAccessKey;" \
                       "SharedAccessKey={key}".format(ns=eventhub_namespace, name=eventhub_enriched, key=eventhub_key)
ehReadConf = {
  'eventhubs.connectionString': readConnectionString
}
ehWriteConf = {
  'eventhubs.connectionString': writeConnectionString
}

# COMMAND ----------

import json
import requests
from collections import OrderedDict
from pyspark.sql.functions import array
from pyspark.sql.types import ArrayType, StringType

# Cognitive Services API connection settings
accessKey = dbutils.preview.secret.get(scope = "storage_scope", key = "textanalytics_key1")
host = dbutils.preview.secret.get(scope = "storage_scope", key = "textanalytics_endpoint")
languagesPath = "/languages"
sentimentPath = "/sentiment"
entitiesPath = "/entities"
languagesUrl = host+languagesPath
sentimenUrl = host+sentimentPath
entitiesUrl = host+entitiesPath

# Handles the call to Cognitive Services API.
# Expects Documents as parameters and the address of the API to call.
# Returns an instance of Documents in response.
def processUsingApi(json, path):
  
  """ A helper function to make the DBFS API request, request/response is encoded/decoded as JSON """
  response = requests.post(
    path,
    headers={
      "Content-Type": "application/json",
      "Ocp-Apim-Subscription-Key": accessKey
    },
    json=json
  )
  return response.json()
  
def getLanguage (text):
  payload={
    "documents": [
      {
        "id": "1",
        "text": text
      }
    ]
  }
  response = processUsingApi(payload, languagesUrl)
  language = "en"
  jsonDoc = next(filter(lambda doc: doc["id"] == "1", response["documents"]))
  if jsonDoc["detectedLanguages"]:
    language = jsonDoc["detectedLanguages"][0]["iso6391Name"]

  return language
  
def getEntities (text):
  language = getLanguage(text)
  payload={
    "documents": [
      {
        "id": "1",
        "language": language,
        "text": text
      }
    ]
  }
  response = processUsingApi(payload, entitiesUrl)
  entities = []
  jsonDoc = next(filter(lambda doc: doc["id"] == "1", response["documents"]))
  for entity in jsonDoc["entities"]:
    entities.append(entity["name"])

  return entities
    
# User Defined Function for processing content of messages to return their sentiment.
def toEntitiesFunc(text):
  
#   language = SentimentDetector.getLanguage(text)
#   if (inputDocs.documents.isEmpty):
#     return "None"
  
  entities = getEntities(text)
  if not entities:
    # Placeholder value to display for no score returned by the sentiment API
    return ["None"]
  else:
    return entities

toEntities = udf(toEntitiesFunc, ArrayType(StringType()))

# COMMAND ----------

def toStringJsonFunc(content, timestamp, entities):
  return str({
    'content': content,
    'timestamp': timestamp,
    'entities': entities
  })
toStringJson = udf(toStringJsonFunc, StringType())

# COMMAND ----------

from pyspark.sql.functions import col, explode, window

df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehReadConf) \
  .load() \
  .selectExpr("cast (body as string) AS Content", "cast (enqueuedTime as timestamp) AS Time") \
  .withColumn("Entities", toEntities(col("Content"))) \
  .withColumn("EventData", toStringJson(col("Content"), col("Time"), col("Entities"))) \
  .selectExpr("EventData as body")
#   .select(col("Content"), col("Time"), explode(col("Entities")).alias("Topic")) \
#   .withWatermark("Time", "10 minute") \
#   .groupBy(window(col("Time"), "10 minutes", "1 minute"), col("Topic")) \
#   .count() \
#   .selectExpr("cast (window.end as timestamp) AS Time", "Topic", "count as Count")

display(df)

# COMMAND ----------

# Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
ds = df \
  .writeStream \
  .format("eventhubs") \
  .options(**ehWriteConf) \
  .option("checkpointLocation", "/mnt/tmp/03_enrichment.chkpnt.tmp") \
  .start()