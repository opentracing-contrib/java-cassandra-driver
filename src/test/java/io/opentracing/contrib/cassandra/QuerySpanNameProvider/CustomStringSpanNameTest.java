package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class CustomStringSpanNameTest {
  @Test
  public void customStringSpanNameTest() {
    QuerySpanNameProvider customStringSpanName = CustomStringSpanName.newBuilder().build();
    assertEquals("execute", customStringSpanName.querySpanName("SELECT * FROM test.table_name;"));

    customStringSpanName = CustomStringSpanName.newBuilder().build(null);
    assertEquals("execute", customStringSpanName.querySpanName("SELECT * FROM test.table_name;"));

    customStringSpanName = CustomStringSpanName.newBuilder().build("CUSTOM");
    assertEquals("CUSTOM", customStringSpanName.querySpanName("SELECT * FROM test.table_name;"));
  }
}
