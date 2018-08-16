// Databricks notebook source
// Retrieve storage credentials
val storage_account = dbutils.preview.secret.get("storage_scope", "storage_account")
val storage_key = dbutils.preview.secret.get("storage_scope", "storage_key")

// Set mount path
val storage_mount_path = "/mnt/blb"

// Unmount if existing
for (mp <- dbutils.fs.mounts()) {
  if (mp.mountPoint == storage_mount_path) {
    dbutils.fs.unmount(storage_mount_path)
  }
}

// Refresh mounts
dbutils.fs.refreshMounts()

// COMMAND ----------

// Mount
dbutils.fs.mount(
  source = "wasbs://databricks@" + storage_account + ".blob.core.windows.net",
  mountPoint = storage_mount_path,
  extraConfigs = Map("fs.azure.account.key." + storage_account + ".blob.core.windows.net" -> storage_key))

// Refresh mounts
dbutils.fs.refreshMounts()

// COMMAND ----------

// ===============================
// Setting up SQL Schema
// ===============================

// COMMAND ----------

import java.sql.DriverManager

var connection:java.sql.Connection = _
var statement:java.sql.Statement = _
   
val serverName = dbutils.preview.secret.get("storage_scope", "sql_server_name") + ".database.windows.net"
val database =dbutils.preview.secret.get("storage_scope", "sql_server_database")
val writeuser = dbutils.preview.secret.get("storage_scope", "sql_admin_login")
val writepwd = dbutils.preview.secret.get("storage_scope", "sql_admin_password")
val tableName = dbutils.preview.secret.get("storage_scope", "DBENV_SQL_TABLE_NAME")
val jdbcPort = dbutils.preview.secret.get("storage_scope", "DBENV_SQL_JDBC_PORT").toInt

val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbc_url = s"jdbc:sqlserver://${serverName}:${jdbcPort};database=${database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
  
Class.forName(driver)
connection = DriverManager.getConnection(jdbc_url, writeuser, writepwd)
statement = connection.createStatement

val ensureStatement = s"""
if not exists (select * from sysobjects where name='${tableName}' and xtype='U')
    create table ${tableName} (
        UniqueId nvarchar(37),
        TweetTime datetime,
        Content text,
        Language nvarchar(10),
        Topic nvarchar(255)
    )
"""

statement.execute(ensureStatement)
connection.close

// COMMAND ----------


