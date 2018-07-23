/*
 * Copyright 2017-2018 The OpenTracing Authors
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
