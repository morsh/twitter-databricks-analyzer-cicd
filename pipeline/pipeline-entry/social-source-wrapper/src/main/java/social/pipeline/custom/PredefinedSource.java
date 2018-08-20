package social.pipeline.custom;

import org.json.JSONArray;
import org.json.JSONObject;
import socialposts.pipeline.sources.SocialMessage;
import socialposts.pipeline.sources.SocialQuery;
import socialposts.pipeline.sources.SocialQueryResult;
import socialposts.pipeline.sources.SocialSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.IOUtils;

/**
 * Source for a set of messages stored in a resource file
 */
public class PredefinedSource implements SocialSource {

  private static final String MESSAGES_FILE = "/messages.json";

  private List<SocialMessage> messages;


  public PredefinedSource() {
    try {
      messages = loadMessagesFromFile(MESSAGES_FILE);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public PredefinedSource(String file) throws IOException {
    messages = loadMessagesFromFile(file);

  }



  public PredefinedSource(List<SocialMessage> messages){
    this.messages = messages;
  }

  private static List<SocialMessage> loadMessagesFromFile(String filename) throws IOException {
    InputStream is =
      PredefinedSource.class.getResourceAsStream(filename);
    String jsonTxt = IOUtils.toString(is);
    JSONArray jsonArray = new JSONArray(jsonTxt);

    List<SocialMessage> messages = new ArrayList();


    Iterator iter = jsonArray.iterator();
    while(iter.hasNext()){
      JSONObject cur = (JSONObject) iter.next();
      messages.add(new SocialMessage((String) cur.get("text")));
    }

    return messages;

  }

  @Override
  public SocialQueryResult search(SocialQuery query) throws Exception {
    CustomSocialQueryResult res = new CustomSocialQueryResult();
    res.setMessages(messages);
    return res;
  }

  public void setMessages(List<SocialMessage> messages){
    this.messages = messages;
  }

    @Override
    public void setOAuthConsumer(String key, String secret) {

  }

  @Override
  public void setOAuthAccessToken(String accessToken, String tokenSecret) {

  }
}
