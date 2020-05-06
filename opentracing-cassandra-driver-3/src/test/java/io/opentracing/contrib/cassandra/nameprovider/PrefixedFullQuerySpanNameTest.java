/*
 * Copyright 2017-2020 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.cassandra.nameprovider;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PrefixedFullQuerySpanNameTest {

  @Test
  public void prefixedFullQuerySpanNameTest() {
    // Default constructor
    QuerySpanNameProvider fullQuerySpanName = PrefixedFullQuerySpanName.newBuilder().build();
    assertEquals("Cassandra: SELECT * FROM test.table_name;",
        fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("Cassandra: N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("Cassandra: N/A", fullQuerySpanName.querySpanName(null));
    // Blank prefix
    fullQuerySpanName = PrefixedFullQuerySpanName.newBuilder().build("");
    assertEquals("SELECT * FROM test.table_name;",
        fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("N/A", fullQuerySpanName.querySpanName(null));
    // Null prefix
    fullQuerySpanName = PrefixedFullQuerySpanName.newBuilder().build(null);
    assertEquals("SELECT * FROM test.table_name;",
        fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("N/A", fullQuerySpanName.querySpanName(null));
    // Custom prefix
    fullQuerySpanName = PrefixedFullQuerySpanName.newBuilder().build("CUSTOM");
    assertEquals("CUSTOM: SELECT * FROM test.table_name;",
        fullQuerySpanName.querySpanName("SELECT * FROM test.table_name;"));
    assertEquals("CUSTOM: N/A", fullQuerySpanName.querySpanName(""));
    assertEquals("CUSTOM: N/A", fullQuerySpanName.querySpanName(null));
  }
}
