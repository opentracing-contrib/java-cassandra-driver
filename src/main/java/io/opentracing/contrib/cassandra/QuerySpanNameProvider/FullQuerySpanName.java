package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

public class FullQuerySpanName implements QuerySpanNameProvider {

  static class Builder implements QuerySpanNameProvider.Builder {
    @Override
    public QuerySpanNameProvider build() { return new io.opentracing.contrib.cassandra.QuerySpanNameProvider.FullQuerySpanName();}

  }

  FullQuerySpanName() {

  }

  @Override
  public String querySpanName(String query) {
    if(query == null || query.equals("")) {
      return "N/A";
    } else {
      return query;
    }
  }

  public static Builder newBuilder() { return new Builder();}
}
