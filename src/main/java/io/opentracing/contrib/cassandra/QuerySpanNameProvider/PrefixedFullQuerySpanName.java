package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

public class PrefixedFullQuerySpanName implements QuerySpanNameProvider {
  private String prefix;

  static class Builder implements QuerySpanNameProvider.Builder {
    @Override
    public QuerySpanNameProvider build() { return new io.opentracing.contrib.cassandra.QuerySpanNameProvider.PrefixedFullQuerySpanName("Cassandra");}

    public QuerySpanNameProvider build(String prefix) {return new io.opentracing.contrib.cassandra.QuerySpanNameProvider.PrefixedFullQuerySpanName(prefix);}
  }

  PrefixedFullQuerySpanName(String prefix) {
    if(prefix == null || prefix.equals("")) {
      this.prefix = "";
    } else {
      this.prefix = prefix + ": ";
    }
  }

  @Override
  public String querySpanName(String query) {
    if(query == null || query.equals("")) {
      return this.prefix + "N/A";
    } else {
      return this.prefix + query;
    }
  }

  public static Builder newBuilder() { return new Builder();}
}
