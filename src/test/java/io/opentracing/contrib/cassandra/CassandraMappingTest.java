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
package io.opentracing.contrib.cassandra;

import static io.opentracing.contrib.cassandra.TestUtil.waitForSpans;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import io.opentracing.Scope;
import io.opentracing.contrib.cassandra.nameprovider.PrefixedFullQuerySpanName;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import java.util.List;
import java.util.UUID;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraMappingTest {

  private static final MockTracer mockTracer = new MockTracer();

  @Before
  public void before() throws Exception {
    System.setProperty("java.library.path", "src/test/resources/libs");
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    Session session = createSession();
    createKeyspace(session);
    createTable(session);
    session.close();
    mockTracer.reset();
  }

  @After
  public void after() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void mapperOperationShouldSetParentCorrectly() {
    Session session = createSession();
    MockSpan parent = mockTracer.buildSpan("parent").start();

    try (Scope ignore = mockTracer.scopeManager().activate(parent, true)) {
      MappingManager mappingManager = new MappingManager(session);
      mappingManager.mapper(Book.class).save(new Book(UUID.randomUUID(), "random book"));
      session.close();
    }

    waitForSpans(mockTracer, 2);

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());

    MockSpan child = findSpanWithNameStartingWith("tested operation");
    assertNotNull(child);

    // TODO: child span should have parent
    // assertEquals(parent.context().spanId(), child.parentId());
  }

  private Session createSession() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(
        builder,
        mockTracer,
        PrefixedFullQuerySpanName.newBuilder().build("tested operation")
    );
    Session session = cluster.connect();
    assertFalse(session.isClosed());
    assertNotNull(session.getState());
    assertNotNull(session.getCluster());
    assertNull(session.getLoggedKeyspace());
    return cluster.connect();
  }

  private void createKeyspace(Session session) {
    PreparedStatement prepared = session.prepare(
        "create keyspace test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
    BoundStatement bound = prepared.bind();
    session.execute(bound);
  }

  private void createTable(Session session) {
    session.execute("CREATE TABLE IF NOT EXISTS test.book (id uuid PRIMARY KEY, title text)");
  }

  private MockSpan findSpanWithNameStartingWith(String operationNamePrefix) {
    for (MockSpan mockSpan : mockTracer.finishedSpans()) {
      if (mockSpan.operationName().startsWith(operationNamePrefix)) {
        return mockSpan;
      }
    }
    return null;
  }

  @Table(name = "book", keyspace = "test")
  public static class Book {

    public Book(UUID id, String title) {
      this.id = id;
      this.title = title;
    }

    @Column
    private UUID id;

    @Column
    private String title;
  }
}
