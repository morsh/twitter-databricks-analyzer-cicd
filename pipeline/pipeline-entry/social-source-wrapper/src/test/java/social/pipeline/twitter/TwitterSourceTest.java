package social.pipeline.twitter;

import org.junit.Before;
import org.junit.Test;
import socialposts.pipeline.sources.SocialMessage;
import socialposts.pipeline.sources.SocialQuery;
import socialposts.pipeline.sources.SocialQueryResult;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class TwitterSourceTest {


  private String twitterConsumerKey;
  private String twitterConsumerSecret;
  private String twitterOauthAccessToken;
  private String twitterOauthTokenSecret;

  @Before
  public void setup() {
    Properties prop = new Properties();
    InputStream input = null;

    try {

      input = new FileInputStream("keys.properties");

      // load a properties file
      prop.load(input);

      twitterConsumerKey = prop.getProperty("consumer_key");
      twitterConsumerSecret = prop.getProperty("consumer_secret");
      twitterOauthAccessToken = prop.getProperty("access_token");
      twitterOauthTokenSecret = prop.getProperty("access_token_secret");


    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test_query() throws Exception {

    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(twitterConsumerKey)
      .setOAuthConsumerSecret(twitterConsumerSecret)
      .setOAuthAccessToken(twitterOauthAccessToken)
      .setOAuthAccessTokenSecret(twitterOauthTokenSecret);



    TwitterSource source = new TwitterSource(cb);
    SocialQueryResult result = source.search(new SocialQuery("russia"));
    List<SocialMessage> messages = result.getMessages();
    messages.stream().forEach(e->System.out.println(e));
  }

}
