import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import socialposts.pipeline.sources.SocialMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class PipelineIntegrationTest {


  private void test() throws IOException {
    List<SocialMessage> testMessages = getTestMessages();

    for(SocialMessage msg : testMessages){
      System.out.println(msg);
    }


  }

  private List<SocialMessage> getTestMessages() throws IOException {
    InputStream is =
      PipelineIntegrationTest.class.getResourceAsStream("/test_messages.json");
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

  public static void main(String[] args) throws IOException {
    PipelineIntegrationTest test = new PipelineIntegrationTest();
    test.test();
  }

}
