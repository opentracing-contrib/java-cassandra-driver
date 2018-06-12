package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

public class CustomStringSpanName implements QuerySpanNameProvider {

  private String customString;

  static class Builder implements QuerySpanNameProvider.Builder {
    // Defaults to "execute"
    @Override
    public QuerySpanNameProvider build() { return new io.opentracing.contrib.cassandra.QuerySpanNameProvider.CustomStringSpanName("execute");}

    // Provide customString
    public QuerySpanNameProvider build(String customString) { return new io.opentracing.contrib.cassandra.QuerySpanNameProvider.CustomStringSpanName(customString);}
  }

  CustomStringSpanName(String customString) {
    if(customString == null) {
      this.customString = "execute";
    } else {
      this.customString = customString;
    }
  }

  @Override
  public String querySpanName(String query) {
    return customString;
  }

  public static Builder newBuilder() { return new Builder();}
}
