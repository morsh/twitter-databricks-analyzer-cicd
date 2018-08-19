package socialposts.pipeline.sources;

import java.util.List;
import java.util.Map;

public class SocialMessage {

  String text;
  long timestamp;
  String source;
  int numOfShares;
  int numOfLikes;
  long id;
  Map<String,Integer> reactions;
  List<String> topics;
  double sentiment;

  public SocialMessage(){

  }

  public SocialMessage(String text, long timestamp){
    this.text = text;
    this.timestamp = timestamp;
  }


  public SocialMessage(String text){
    this.text = text;
    this.timestamp = System.currentTimeMillis();
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public int getNumOfShares() {
    return numOfShares;
  }

  public void setNumOfShares(int numShared) {
    numOfShares = numShared;
  }

  public int getNumOfLikes() {
    return numOfLikes;
  }

  public void setNumOfLikes(int numOfLikes) {
    this.numOfLikes = numOfLikes;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public Map<String, Integer> getReactions() {
    return reactions;
  }

  public void setReactions(Map<String, Integer> reactions) {
    this.reactions = reactions;
  }

  public List<String> getTopics() {
    return topics;
  }

  public void setTopics(List<String> topics) {
    this.topics = topics;
  }

  public double getSentiment() {
    return sentiment;
  }

  public void setSentiment(double sentiment) {
    this.sentiment = sentiment;
  }

  @Override
  public String toString() {
    return "SocialMessage{" +
      "text='" + text + '\'' +
      ", timestamp=" + timestamp +
      '}';
  }
}
