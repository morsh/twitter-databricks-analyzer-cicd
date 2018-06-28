# Databricks notebook source
# Global imports...
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Set up the connection parameters...
# TODO: Replace the values below from your IoT Hub's EventHub compatible endpoint...
eventhub_namespace = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_namespace")
eventhub_input = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_input")
eventhub_key = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_key")
readConnectionString = "Endpoint=sb://{ns}.servicebus.windows.net/;" \
                       "EntityPath={name};SharedAccessKeyName=RootManageSharedAccessKey;" \
                       "SharedAccessKey={key}".format(ns=eventhub_namespace, name=eventhub_input, key=eventhub_key)
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
challenge3 = bodyWithSchema \
  .withColumn("eventId", col("data.eventId")) \
  .withColumn("dataItemId", col("data.dataItemId")) \
  .withColumn("eventName", col("data.eventName")) \
  .withColumn("eventTopic", col("data.eventTopic")) \
  .withColumn("category", col("data.data.category")) \
  .withColumn("sentiment", col("data.data.sentiment")) \
  .withColumn("location", col("data.data.location.name")) \
  .withColumn("location_entity", col("data.data.location.entity")) \
  .withColumn("eventTime", col("Time").cast(TimestampType()))

display(challenge3)
  
challenge3.createOrReplaceTempView("ChallengeData")

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql.DriverManager
# MAGIC import org.apache.spark.sql.ForeachWriter
# MAGIC 
# MAGIC // Create the query to be persisted...
# MAGIC val dfPersistence = spark.sql("Select category, window.start as windowStart, count(*) as ItemCount from ChallengeData GROUP BY category, WINDOW(Time, '5 minutes')")
# MAGIC 
# MAGIC display(dfPersistence)
# MAGIC 
# MAGIC // Writing to SQL
# MAGIC val persistenceQuery = dfPersistence.writeStream.foreach(new ForeachWriter[Row] {
# MAGIC   var connection:java.sql.Connection = _
# MAGIC   var statement:java.sql.Statement = _
# MAGIC    
# MAGIC   // TODO: Replace these values as necessary...
# MAGIC   val tableName = "dbo.EnrichedItems"
# MAGIC   val serverName = "social-data-server.database.windows.net"
# MAGIC   val jdbcPort = 1433
# MAGIC   val database ="social-data"
# MAGIC   val writeuser = "zcadmin"
# MAGIC   val writepwd = "Password1"
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
# MAGIC     val category = value(0)
# MAGIC     val windowStartTime = value(1)
# MAGIC     val itemCount = value(2)
# MAGIC     
# MAGIC     val valueStr = s"'${category}', '${windowStartTime}', ${itemCount}"
# MAGIC     val statementStr = s"INSERT INTO ${tableName} (Category, windowStart, ItemCount) VALUES (${valueStr})"
# MAGIC     
# MAGIC     statement.execute(statementStr)
# MAGIC   }
# MAGIC 
# MAGIC   def close(errorOrNull: Throwable): Unit = {
# MAGIC     connection.close
# MAGIC   }
# MAGIC })
# MAGIC 
# MAGIC val streamingQuery = persistenceQuery.start()

# COMMAND ----------

# MAGIC %scala
# MAGIC streamingQuery.stop()

# COMMAND ----------


