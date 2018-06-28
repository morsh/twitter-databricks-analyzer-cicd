# Databricks notebook source
def readSQlTable(table):
  driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
  url = 'jdbc:sqlserver://fleetsqlsrv.database.windows.net:1433;database=PILDW'
  user = 'fleetadmin'
  password = '3edc$RFV#'
  dataframe = spark.read.format("jdbc")\
    .option("driver", driver)\
    .option("url", url)\
    .option("dbtable", table)\
    .option("user", user)\
    .option("password", password)\
    .load()
  dataframe.createOrReplaceTempView(table)
  return dataframe

# COMMAND ----------

#mode='append' | 'overwrite'
def writeToSQlTable(table, df, mode='append'):
  driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
  url = 'jdbc:sqlserver://fleetsqlsrv.database.windows.net:1433;database=PILDW'
  user = 'fleetadmin'
  password = '3edc$RFV#'
  df.write.format('jdbc')\
    .option("driver", driver)\
    .option("url", url)\
    .option("dbtable", table)\
    .option("user", user)\
    .option("password", password)\
    .mode(mode)\
    .save()

# COMMAND ----------

df = readSQlTable('GMUTXReason')
writeToSQlTable('GMUTXReason',df,'overwrite')

# COMMAND ----------

display(readSQlTable('MLSubscriberSegment'))

# COMMAND ----------

display(readSQlTable('xVehicleType'))

# COMMAND ----------

display(readSQlTable('MLModelScores'))
