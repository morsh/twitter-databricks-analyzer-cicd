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


