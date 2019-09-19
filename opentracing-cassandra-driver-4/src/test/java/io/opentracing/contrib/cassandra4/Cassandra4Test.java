/*
 * Copyright 2017-2019 The OpenTracing Authors
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
package io.opentracing.contrib.cassandra4;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.utils.UUIDGen;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Cassandra4Test {
  private static final MockTracer tracer = new MockTracer();

  @Before
  public void before() {
    tracer.reset();

  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("java.library.path", "src/test/resources/libs");
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
  }

  @AfterClass
  public static void afterClass() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void sync() {
    CqlSession session = createSession();
    createKeyspace(session);
    createTable(session);
    insert(session);
    session.close();

    List<MockSpan> finished = tracer.finishedSpans();
    assertEquals(4, finished.size());

    checkSpans(finished);

    assertNull(tracer.activeSpan());
  }

  @Test
  public void async() throws Exception {
    CqlSession session = createSessionAsync();
    createKeyspaceAsync(session);
    createTableAsync(session);
    insertAsync(session);
    session.closeAsync().toCompletableFuture().get(15, TimeUnit.SECONDS);

    List<MockSpan> finished = tracer.finishedSpans();
    assertEquals(4, finished.size());

    checkSpans(finished);

    assertNull(tracer.activeSpan());
  }

  private CqlSession createSession() {
    return new TracingCqlSession(CqlSession.builder()
        .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9142)))
        .withLocalDatacenter("datacenter1")
        .build(), tracer);
  }

  private CqlSession createSessionAsync() throws Exception {
    return new TracingCqlSession(CqlSession.builder()
        .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9142)))
        .withLocalDatacenter("datacenter1")
        .buildAsync().toCompletableFuture().get(15, TimeUnit.SECONDS), tracer);
  }

  private void createKeyspace(CqlSession session) {
    PreparedStatement prepared = session.prepare(
        "create keyspace sync WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
    BoundStatement bound = prepared.bind();
    session.execute(bound);
  }

  private void createKeyspaceAsync(CqlSession session) throws Exception {
    PreparedStatement prepared = session.prepareAsync(
        "create keyspace async WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};")
        .toCompletableFuture()
        .get(15, TimeUnit.SECONDS);
    BoundStatement bound = prepared.bind();
    session.executeAsync(bound).toCompletableFuture().get(15, TimeUnit.SECONDS);
  }

  private void createTable(CqlSession session) {
    session.execute("CREATE TABLE IF NOT EXISTS sync.book (id uuid PRIMARY KEY, title text)");
  }

  private void createTableAsync(CqlSession session) throws Exception {
    session.executeAsync("CREATE TABLE IF NOT EXISTS async.book (id uuid PRIMARY KEY, title text)")
        .toCompletableFuture().get(15, TimeUnit.SECONDS);
  }

  private void insert(CqlSession session) {
    // Insert 1
    final SimpleStatement statement = QueryBuilder.insertInto("sync", "book")
        .value("id", literal(UUIDGen.getTimeUUID()))
        .value("title", literal("title"))
        .build();
    session.execute(statement);

    // Insert 2
    PreparedStatement preparedStatement = session.prepare(
        SimpleStatement.newInstance("INSERT INTO sync.book (id, title) VALUES (?, ?)"));
    session.execute(preparedStatement.bind(UUIDGen.getTimeUUID(), "title3"));
  }

  private void insertAsync(CqlSession session) throws Exception {
    // Insert 1
    final SimpleStatement statement = QueryBuilder.insertInto("async", "book")
        .value("id", literal(UUIDGen.getTimeUUID()))
        .value("title", literal("title"))
        .build();
    session.executeAsync(statement).toCompletableFuture().get(15, TimeUnit.SECONDS);

    // Insert 2
    PreparedStatement preparedStatement = session.prepareAsync(
        SimpleStatement.newInstance("INSERT INTO async.book (id, title) VALUES (?, ?)"))
        .toCompletableFuture().get(15, TimeUnit.SECONDS);
    session.executeAsync(preparedStatement.bind(UUIDGen.getTimeUUID(), "title3"))
        .toCompletableFuture().get(15, TimeUnit.SECONDS);
  }

  private void checkSpans(List<MockSpan> mockSpans) {
    for (MockSpan mockSpan : mockSpans) {
      assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
      assertEquals(TracingCqlSession.COMPONENT_NAME, mockSpan.tags().get(Tags.COMPONENT.getKey()));
      assertEquals("cassandra", mockSpan.tags().get(Tags.DB_TYPE.getKey()));
      assertEquals(0, mockSpan.generatedErrors().size());
      String operationName = mockSpan.operationName();
      assertEquals("execute", operationName);
    }
  }
}
