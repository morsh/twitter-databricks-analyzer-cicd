package social.pipeline.custom;

import socialposts.pipeline.sources.SocialMessage;
import socialposts.pipeline.sources.SocialQuery;
import socialposts.pipeline.sources.SocialQueryResult;
import socialposts.pipeline.sources.SocialSource;

import java.util.List;

public class CustomSource implements SocialSource {

  private List<SocialMessage> messages;

  public CustomSource(){

  }
  public CustomSource(List<SocialMessage> messages){
    this.messages = messages;
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
