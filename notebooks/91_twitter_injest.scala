// Databricks notebook source
// Databricks notebook source
import java.util._
import java.util.concurrent._
import com.microsoft.azure.eventhubs._
import scala.collection.JavaConverters._

val eventhub_namespace = dbutils.preview.secret.get("storage_scope", "eventhub_namespace")
val eventhub_input = dbutils.preview.secret.get("storage_scope", "eventhub_input")
val eventhub_key = dbutils.preview.secret.get("storage_scope", "eventhub_key")
val eventhub_keyname = "RootManageSharedAccessKey"
val connStr = new ConnectionStringBuilder()
            .setNamespaceName(eventhub_namespace)
            .setEventHubName(eventhub_input)
            .setSasKeyName(eventhub_keyname)
            .setSasKey(eventhub_key)

val pool = Executors.newFixedThreadPool(1)
val eventHubClient = EventHubClient.create(connStr.toString(), pool)

def sendEvent(message: String) = {
  val messageData = EventData.create(message.getBytes("UTF-8"))
  eventHubClient.get().send(messageData)
  System.out.println("Sent event: " + message + "\n")
}

import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

// Twitter configuration!

val twitterConsumerKey = dbutils.preview.secret.get("storage_scope", "DBENV_TWITTER_CONSUMER_KEY")
val twitterConsumerSecret = dbutils.preview.secret.get("storage_scope", "DBENV_TWITTER_CONSUMER_SECRET")
val twitterOauthAccessToken = dbutils.preview.secret.get("storage_scope", "DBENV_TWITTER_OAUTH_ACCESS_TOKEN")
val twitterOauthTokenSecret = dbutils.preview.secret.get("storage_scope", "DBENV_TWITTER_OAUTH_TOKEN_SECRET")

val cb = new ConfigurationBuilder()
  cb.setDebugEnabled(true)
  .setOAuthConsumerKey(twitterConsumerKey)
  .setOAuthConsumerSecret(twitterConsumerSecret)
  .setOAuthAccessToken(twitterOauthAccessToken)
  .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

val twitterFactory = new TwitterFactory(cb.build())
val twitter = twitterFactory.getInstance()

// Getting tweets with keyword "Azure" and sending them to the Event Hub in realtime!

// val query = new Query(" #harrypotter ")
val query = new Query("russia")
query.setCount(100)
query.lang("en")
var finished = false
while (!finished) {
  val result = twitter.search(query)
  val statuses = result.getTweets()
  var lowestStatusId = Long.MaxValue
  for (status <- statuses.asScala) {
    if(!status.isRetweet()){
      sendEvent(status.getText())
    }
    lowestStatusId = Math.min(status.getId(), lowestStatusId)
    Thread.sleep(2000)
  }
  query.setMaxId(lowestStatusId - 1)
}

// Closing connection to the Event Hub
eventHubClient.get().close()

// COMMAND ----------


