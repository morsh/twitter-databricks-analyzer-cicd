# Databricks notebook source
# Global imports...
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Set up the connection parameters...
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

# Connect to the IoT Hub...
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
itemHistoryDF = bodyWithSchema \
  .withColumn("eventId", col("data.eventId")) \
  .withColumn("dataItemId", col("data.dataItemId")) \
  .withColumn("eventName", col("data.eventName")) \
  .withColumn("eventTopic", col("data.eventTopic")) \
  .withColumn("category", col("data.data.category")) \
  .withColumn("sentiment", col("data.data.sentiment")) \
  .withColumn("location", col("data.data.location.name")) \
  .withColumn("location_entity", col("data.data.location.entity")) \
  .withColumn("eventTime", col("Time").cast(TimestampType()))

display(itemHistoryDF)
  
itemHistoryDF.createOrReplaceTempView("ItemHistory")

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql.DriverManager
# MAGIC import org.apache.spark.sql.ForeachWriter
# MAGIC 
# MAGIC // Create the query to be persisted...
# MAGIC val dfItemHistory = spark.sql("SELECT body, eventId, dataItemId, eventName, eventTopic, category, sentiment, location, location_entity, eventTime FROM ItemHistory")
# MAGIC 
# MAGIC display(dfItemHistory)
# MAGIC 
# MAGIC // Writing to SQL
# MAGIC val historyQuery = dfItemHistory.writeStream.foreach(new ForeachWriter[Row] {
# MAGIC   var connection:java.sql.Connection = _
# MAGIC   var statement:java.sql.Statement = _
# MAGIC    
# MAGIC   // TODO: Replace these values as necessary...
# MAGIC   val tableName = dbutils.preview.secret.get("storage_scope", "DBENV_SQL_TABLE_NAME")
# MAGIC   val serverName = dbutils.preview.secret.get("storage_scope", "DBENV_SQL_SERVER_NAME")
# MAGIC   val jdbcPort = dbutils.preview.secret.get("storage_scope", "DBENV_SQL_JDBC_PORT").toInt
# MAGIC   val database =dbutils.preview.secret.get("storage_scope", "DBENV_SQL_DATABASE")
# MAGIC   val writeuser = dbutils.preview.secret.get("storage_scope", "DBENV_SQL_USER")
# MAGIC   val writepwd = dbutils.preview.secret.get("storage_scope", "DBENV_SQL_PASSWORD")
# MAGIC 
# MAGIC   val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC   val jdbc_url = s"jdbc:sqlserver://${serverName}:${jdbcPort};database=${database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# MAGIC   
# MAGIC   def open(partitionId: Long, version: Long):Boolean = {
# MAGIC     Class.forName(driver)
# MAGIC     connection = DriverManager.getConnection(jdbc_url, writeuser, writepwd)
# MAGIC     statement = connection.createStatement
# MAGIC     true
# MAGIC   }
# MAGIC   
# MAGIC   def process(value: Row): Unit = {
# MAGIC     val body = (value(0) + "").replaceAll("'", "''")
# MAGIC     val eventId = value(1)
# MAGIC     val dataItemId = value(2)
# MAGIC     val eventName = value(3)
# MAGIC     val eventTopic = value(4)
# MAGIC     val category = value(5)
# MAGIC     val sentiment = value(6)
# MAGIC     val location = (value(7) + "").replaceAll("'", "''")
# MAGIC     val location_entity = (value(8) + "").replaceAll("'", "''")
# MAGIC     val eventTime = value(9)
# MAGIC     
# MAGIC     val valueStr = s"'${body}', '${eventId}', '${dataItemId}', '${eventName}', '${eventTopic}', '${category}', ${sentiment}, '${location}', '${location_entity}', '${eventTime}'"
# MAGIC     val statementStr = s"INSERT INTO ${tableName} (body, eventId, dataItemId, eventName, eventTopic, category, sentiment, location, location_entity, eventTime) VALUES (${valueStr})"
# MAGIC     
# MAGIC     statement.execute(statementStr)
# MAGIC   }
# MAGIC 
# MAGIC   def close(errorOrNull: Throwable): Unit = {
# MAGIC     connection.close
# MAGIC   }
# MAGIC })
# MAGIC 
# MAGIC val streamingQuery = historyQuery.start()

# COMMAND ----------

# MAGIC %scala
# MAGIC streamingQuery.stop()

# COMMAND ----------


