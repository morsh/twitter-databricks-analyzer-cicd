package socialposts.pipeline.sources;

public class SocialQuery {

  private String query;
  private String lang;

  public SocialQuery() {
  }

  public SocialQuery(String query) {
    this.query = query;
  }

  public String getLang() {
    return this.lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }


  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }
}
