# Databricks notebook source
import uuid
import datetime
import random

namespaceName = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_namespace")
eventHubName = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_ratings")
sasKeyName = "RootManageSharedAccessKey"
sasKey = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_ratings_key")

# Source with default settings
connectionString = f"Endpoint=sb://{namespaceName}.servicebus.windows.net/;EntityPath={eventHubName};SharedAccessKeyName={sasKeyName};SharedAccessKey={sasKey}"
ehConf = {
  'eventhubs.connectionString' : connectionString
}

df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()
  
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

from pyspark.sql.functions import col, explode, window

topicsDF = df \
  .selectExpr("cast (body as string) AS Content", "cast (enqueuedTime as timestamp) AS Time") \
  .withColumn("Entities", toEntities(col("Content"))) \
  .select(col("Content"), col("Time"), explode(col("Entities")).alias("Topic")) \
  .withWatermark("Time", "10 minute") \
  .groupBy(window(col("Time"), "10 minutes", "1 minute"), col("Topic")) \
  .count() \
  .selectExpr("cast (window.end as timestamp) AS Time", "Topic", "count as Count")
  
  
# query = topicsDF.writeStream.outputMode("append").format("console").option("truncate", "false").start()

# query.awaitTermination()

display(topicsDF)

# COMMAND ----------

# # topicsDF.write.csv('/mnt/dbpath/topics.csv', 'data')
# query = topicsDF.writeStream \
#     .format("csv") \
#     .outputMode('append') \
#     .option('path', '/mnt/dbpath/topics_save_csv') \
#     .option('checkpointLocation', '/mnt/dbpath/topics_checkpointq.tmp') \
#     .start()

# # topicsDF.writeStream.outputMode("append").format("console").option("truncate", "False").start().awaitTermination()

# query.awaitTermination()

# COMMAND ----------


