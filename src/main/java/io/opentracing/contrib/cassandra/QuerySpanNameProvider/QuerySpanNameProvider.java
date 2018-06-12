package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

public interface QuerySpanNameProvider {
  interface Builder {
    QuerySpanNameProvider build();
  }

  String querySpanName(String query);

}
