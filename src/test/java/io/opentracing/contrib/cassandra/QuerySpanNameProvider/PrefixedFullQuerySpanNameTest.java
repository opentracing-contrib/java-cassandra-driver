package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrefixedFullQuerySpanNameTest {
  @Test
  public void prefixedFullQuerySpanNameTest() {
    // Default constructor
    QuerySpanNameProvider fullQuerySpanName = PrefixedFullQuerySpanName.newBuilder().build();
    assertEquals("Cassandra: SELECT * FROM test.table_name;", fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("Cassandra: N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("Cassandra: N/A", fullQuerySpanName.querySpanName(null));
    // Blank prefix
    fullQuerySpanName = PrefixedFullQuerySpanName.newBuilder().build("");
    assertEquals("SELECT * FROM test.table_name;", fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("N/A", fullQuerySpanName.querySpanName(null));
    // Null prefix
    fullQuerySpanName = PrefixedFullQuerySpanName.newBuilder().build(null);
    assertEquals("SELECT * FROM test.table_name;", fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("N/A", fullQuerySpanName.querySpanName(null));
    // Custom prefix
    fullQuerySpanName = PrefixedFullQuerySpanName.newBuilder().build("CUSTOM");
    assertEquals("CUSTOM: SELECT * FROM test.table_name;", fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("CUSTOM: N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("CUSTOM: N/A", fullQuerySpanName.querySpanName(null));
  }
}
