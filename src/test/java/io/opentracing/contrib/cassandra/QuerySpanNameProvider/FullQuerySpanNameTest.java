package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FullQuerySpanNameTest {
  @Test
  public void fullQuerySpanNameTest() {
    QuerySpanNameProvider fullQuerySpanName = FullQuerySpanName.newBuilder().build();
    assertEquals("SELECT * FROM test.table_name;", fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("N/A", fullQuerySpanName.querySpanName(null));
  }
}
