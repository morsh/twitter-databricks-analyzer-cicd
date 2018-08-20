package socialposts.pipeline.sources;

import social.pipeline.custom.PredefinedSource;
import social.pipeline.twitter.TwitterSource;

public class SocialSourceFactory {

  private SocialSourceFactory(){

  }


  public static SocialSource getSocialSource(SourceName name){
    switch(name){
      case CUSTOM:
        return new PredefinedSource();
      case TWITTER:
        return new TwitterSource()
    }
  }

}
