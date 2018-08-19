package socialposts.pipeline.sources;

import java.util.List;

public interface SocialQueryResult {

  List<SocialMessage> getMessages();
  String getQuery();
  SocialQuery nextQuery();
  boolean hasNext();
}
