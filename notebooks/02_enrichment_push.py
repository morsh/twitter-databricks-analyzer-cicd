# Databricks notebook source
import requests
from pyspark.sql import *
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

# Creating the schema to use when reading JSON from event hub
data_item_schema = (StructType()
  .add("_id", StringType())
  .add("sourceId", StringType())
  .add("itemType", StringType())
  .add("itemSubType", StringType())
  .add("sourceCreatedAt", StringType())
  .add("content", StringType())
  .add("client", StringType())
  .add("category", StringType(), nullable=True)
  .add("sentiment", FloatType(), nullable=True)
  .add("aiCategory", StringType(), nullable=True)
  .add("aiSentiment", FloatType(), nullable=True)
  .add("dataSource", StringType())
  .add("sourceTotalLike", IntegerType())
  .add("sourceTotalComments", IntegerType())
  .add("parent", StringType(), nullable=True)
  .add("status", StringType(), nullable=True)
  .add("location", StringType(), nullable=True)
  .add("official", BooleanType())
  .add("sentimentStatus", StringType())
  .add("categoryStatus", StringType())
  .add("locationStatus", StringType())
  .add("sentimentReason", StringType(), nullable=True)
  .add("categoryReason", StringType(), nullable=True)
  .add("locationReason", StringType(), nullable=True)
  .add("contextCategory", StringType(), nullable=True)
  .add("language", StringType(), nullable=True)
  .add("sourceEngagements", StructType(), nullable=True)
  .add("threadSummary", StringType())
  .add("threadStartedAt", StringType())
  .add("threadEndedAt", StringType())
  .add("threadRoot", StringType())
  .add("locations", ArrayType(StringType()))
  .add("classificationError", StringType(), nullable=True)
  .add("nerError", StringType(), nullable=True)
  )

# Creating the schema for an event
data_item_event_schema = (StructType()
  .add("data", data_item_schema)
  .add("eventId", StringType())
  .add("eventName", StringType())
  .add("eventTopic", StringType())
 )

# COMMAND ----------

configuration = {
    "staging": {
        "language_detection_service": "http://services:fAs3v58RXTgPl@staging-lang-detect-svc.zencity.io",
        "ner_service": "http://services:fAs3v58RXTgPl@staging-ner-svc.zencity.io",
        "classification_service": "https://ai.zencity.io/api/analyze_categorize_simple",
    },
    "paths": {
        "language_path": "/api/v1/detect",
        "ner_path": "/api/recognize",
        "classification_path": ""
    }
}

def cast(to, with_mapping):
  return lambda x: x if isinstance(x, to) else with_mapping[type(x)](x)

to_dict = cast(dict, {Row: lambda x: x.asDict()})

# Merge results from two different JSON object into one object
def merge_result(fn):
  return lambda data_item: dict(to_dict(data_item), **fn(data_item))

# Handles the call to Cognitive Services API.
# Expects Documents as parameters and the address of the API to call.
# Returns an instance of Documents in response.
def make_api_request(json, path):
    """ A helper function to make the DBFS API request, request/response is encoded/decoded as JSON """
    response = requests.post(
        path,
        headers={
            "Content-Type": "application/json",
        },
        json=json
    )
    return response  # .json()


def get_language(dataitem):
    payload = {
        "content": dataitem.content
    }
    try:
        language_url = (configuration.get("staging", {}).get("language_detection_service")
                        + configuration.get("paths", {}).get("language_path"))
        response = make_api_request(payload, language_url)
        return response.json().get('data', {}).get('language')
    except Exception as e:
        return None

to_language = udf(merge_result(lambda dataitem: {"language": dataitem.language if dataitem.language else get_language(dataitem)}) , data_item_schema)

def get_classification(dataitem):
    try:
        payload = {
            'message': dataitem.content,
            'data_item': dataitem.asDict(True),
            'default_category': dataitem.contextCategory or 'no_category'
        }
        classification_url = (configuration.get("staging").get("classification_service")
                              + configuration.get("paths").get("classification_path"))
        response = make_api_request(payload, classification_url)
        data = response.json()
        category_result = {
            "category": data.get("category", "other"),
            "aiCategory": data.get("category", "other"),
            "categoryStatus": "pending-moderation",
            "categoryReason": "ai",
            "pendingDerive": True
        } if not data.get("category_error", False) else {
            "categoryStatus": "ai-failed",
            "categoryError": data.get("category_error")
        }
        sentiment_result = {
            "sentiment": data.get("compound_sentiment", {}).get("compound"),
            "aiSentiment": data.get("compound_sentiment", {}).get("compound"),
            "sentimentStatus": "pending-moderation",
            "sentimentReason": "ai",
        } if not data.get("sentiment_error", False) else {
            "sentimentStatus": "ai-failed",
            "sentimentError": data.get("sentiment_error")
        }
        return dict(category_result, **sentiment_result)
    except Exception as e:
        return {'classificationError': str(e)}

# Creating schema and UDF for classification extraction
#=======================================================

classification_schema = (StructType()
   .add("sentiment", FloatType(), nullable=True)
   .add("aiSentiment", FloatType(), nullable=True)
   .add("sentimentReason", StringType())
   .add("sentimentStatus", StringType())
   .add("sentimentError", StringType(), nullable=True)
   .add("category", StringType(), nullable=True)
   .add("aiCategory", StringType(), nullable=True)
   .add("categoryStatus", StringType())
   .add("categoryReason", StringType())
   .add("categoryError", StringType(), nullable=True)
   .add("classificationError", StringType(), nullable=True)
   )

def enrich_classification(data_item):
    if data_item["content"] \
            and (data_item["itemSubType"] != "engagements") \
            and (data_item["categoryStatus"] == "pending-ai"
            or data_item["sentimentStatus"] == "pending-ai"
            or not data_item["categoryStatus"]
            or not data_item["sentimentStatus"]):
        return get_classification(data_item)
    elif not data_item["content"]:
        return {
            "categoryStatus": "blank",
            "categoryReason": "blank",
            "sentimentStatus": "blank",
            "sentimentReason": "blank"
        }
    else:
        return {
            "category": data_item["category"],
            "aiCategory": data_item["aiCategory"],
            "categoryStatus": data_item["categoryStatus"],
            "categoryReason": data_item["categoryReason"],
            "sentiment": data_item["sentiment"],
            "aiSentiment": data_item["aiSentiment"],
            "sentimentStatus": data_item["sentimentStatus"],
            "sentimentReason": data_item["sentimentReason"],
        }

to_classification = udf(merge_result(enrich_classification), data_item_schema)

# Creating schema and UDF for NER (Name Entity Recognition) extraction
#======================================================================

def get_ner(data_item):
    try:
        payload = {
            "client": {
                "id": data_item["client"],
                "name": data_item["clientName"]
                # TODO the service need to access client territory data
            },
            "content": data_item["content"]
        }
        url = configuration.get("staging").get("ner_service") + configuration.get("paths").get("ner_path")
        response = make_api_request(payload, url)
        data = response.json()
        if data.get("error"):
          return {
            "locationStatus": "ai-failed",
            "locationReason": "ai",
            "nerError": data["error"]
          }
        elif data.get("locations"):
          return {
            "locations": data["locations"],
            "locationStatus": "pending-moderation",
            "locationReason": "ai"
          }
        else:
          return {
            "locationStatus": "ai-blank",
            "locationReason": "ai"
          }
    except Exception as e:
        return {"nerError": str(e), "locationStatus": "ai-failed", "locationReason": "ai"}


def enrichment_ner(data_item):
    if data_item["locationStatus"] == "pending-ai":
      return get_ner(data_item)
    else:
      return {}

ner_schema = (StructType()
  .add('locations', ArrayType(StringType()))
  .add('locationStatus', StringType())
  .add('locationReason', StringType())
  .add('nerError', StringType(), nullable=True)
  )


to_ner = udf(merge_result(enrichment_ner), data_item_schema)



# COMMAND ----------

df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehReadConf) \
  .load() \
  .selectExpr("cast (body as string) AS Content", "cast (enqueuedTime as timestamp) AS Time") \
  .withColumn("EventData", from_json(col("Content"), data_item_event_schema)) \
  .withColumn("EventData", col("EventData.data")) \
  .withColumn("EventData", to_language(col("EventData"))) \
  .withColumn("EventData", to_classification(col("EventData"))) \
  .withColumn("EventData", to_ner(col("EventData"))) \
  .withColumn("Engagements", col("EventData.sourceEngagements")) \
  .withColumn("EventData", to_json(col("EventData"))) \
  .selectExpr("EventData as body")

display(df)   

# Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
ds = df \
  .writeStream \
  .format("eventhubs") \
  .options(**ehWriteConf) \
  .option("checkpointLocation", "/mnt/tmp/02_enrichment_checkpoint.tmp") \
  .start()

