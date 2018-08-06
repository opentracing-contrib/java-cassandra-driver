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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentracing.Scope;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraTest {

  private static final MockTracer mockTracer = new MockTracer();

  @Before
  public void before() throws Exception {
    System.setProperty("java.library.path", "src/test/resources/libs");
    mockTracer.reset();
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
  }

  @After
  public void after() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void withoutParentAsync() throws Exception {
    Session session = createSession();
    createKeyspaceAsync(session);
    createTableAsync(session);
    insertAsync(session);
    session.closeAsync().get(15, TimeUnit.SECONDS);

    waitForSpans(mockTracer, 5);

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(5, finished.size());

    for (MockSpan mockSpan : finished) {
      assertEquals(0, mockSpan.parentId());
    }

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void withoutParent() {
    Session session = createSession();
    createKeyspace(session);
    createTable(session);
    insert(session);
    session.close();

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(5, finished.size());

    for (MockSpan mockSpan : finished) {
      assertEquals(0, mockSpan.parentId());
    }

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void badQuery() {
    Session session = createSession();
    try {
      session.execute("bad query");
      fail();
    } catch (Exception ignored) {
    }

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    MockSpan span = finished.get(0);
    checkSpanWithError(span);
  }

  @Test
  public void badQueryAsync() {
    Session session = createSession();
    try {
      session.executeAsync("bad query").get(15, TimeUnit.SECONDS);
      fail();
    } catch (Exception ignored) {
    }

    waitForSpans(mockTracer, 1);
    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    MockSpan span = finished.get(0);
    checkSpanWithError(span);
  }

  @Test
  public void badQueryWithArgs() {
    Session session = createSession();
    try {
      session.execute("bad query", "arg");
      fail();
    } catch (Exception ignored) {
    }

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    MockSpan span = finished.get(0);
    checkSpanWithError(span);
  }

  @Test
  public void badQueryWithArgsAsync() {
    Session session = createSession();
    try {
      session.executeAsync("bad query", "arg").get(15, TimeUnit.SECONDS);
      fail();
    } catch (Exception ignored) {
    }

    waitForSpans(mockTracer, 1);
    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    MockSpan span = finished.get(0);
    checkSpanWithError(span);
  }

  @Test
  public void badQueryWithMap() {
    Session session = createSession();
    try {
      session.execute("bad query", new HashMap<String, Object>());
      fail();
    } catch (Exception ignored) {
    }

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    MockSpan span = finished.get(0);
    checkSpanWithError(span);
  }

  @Test
  public void badQueryWithMapAsync() {
    Session session = createSession();
    try {
      session.executeAsync("bad query", new HashMap<String, Object>()).get(15, TimeUnit.SECONDS);
      fail();
    } catch (Exception ignored) {
    }

    waitForSpans(mockTracer, 1);
    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    MockSpan span = finished.get(0);
    checkSpanWithError(span);
  }

  @Test
  public void badQueryWithStatement() {
    Session session = createSession();
    createKeyspace(session);
    createTable(session);
    PreparedStatement prepared = session.prepare("INSERT INTO test.book (id, title) VALUES (?, ?)");
    BoundStatement bound = prepared.bind();
    mockTracer.reset();
    try {
      session.execute(bound);
      fail();
    } catch (Exception ignored) {
    }

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    MockSpan span = finished.get(0);
    checkSpanWithError(span);
  }

  @Test
  public void badQueryWithStatementAsync() {
    Session session = createSession();
    createKeyspace(session);
    createTable(session);
    PreparedStatement prepared = session.prepare("INSERT INTO test.book (id, title) VALUES (?, ?)");
    BoundStatement bound = prepared.bind();
    mockTracer.reset();
    try {
      session.executeAsync(bound).get(15, TimeUnit.SECONDS);
      fail();
    } catch (Exception ignored) {
    }

    waitForSpans(mockTracer, 1);
    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    MockSpan span = finished.get(0);
    checkSpanWithError(span);
  }

  @Test
  public void withParent() throws Exception {
    Session session = createSession();

    Scope parentSpan = mockTracer.buildSpan("parent").startActive(true);

    session.executeAsync("SELECT * FROM system_schema.keyspaces;").get();
    session.execute("SELECT * FROM system_schema.keyspaces;");
    parentSpan.close();

    waitForSpans(mockTracer, 3);

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(3, finished.size());
    MockSpan parent = (MockSpan) parentSpan.span();
    List<MockSpan> childSpans = new ArrayList<>();
    for (MockSpan mockSpan : finished) {
      if (mockSpan.context().spanId() == parent.context().spanId()) {
        // skip parent span
        continue;
      }
      childSpans.add(mockSpan);
      assertEquals(parent.context().spanId(), mockSpan.parentId());
      assertEquals(parent.context().traceId(), mockSpan.context().traceId());
    }
    assertEquals(2, childSpans.size());

    checkSpans(childSpans);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void queryBuilder() {
    Session session = createSession();
    Select select = QueryBuilder.select().from("system_schema", "keyspaces");
    session.execute(select);

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void usingNewSession() throws Exception {
    Session session = createNewSession();
    session = session.initAsync().get(15, TimeUnit.SECONDS);

    session.execute("SELECT * FROM system_schema.keyspaces;");

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void newSessionWithKey() {
    Session session = createSessionWithKey();
    session = session.init();
    session.close();
    assertTrue(session.isClosed());
    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(0, finished.size());
  }

  @Test
  public void connectAsync() throws ExecutionException, InterruptedException {
    ListenableFuture<Session> futureSession = createAsyncSession();
    Session session = futureSession.get();

    session.execute("SELECT * FROM system_schema.keyspaces;");

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void connectAsyncWithKey() throws Exception {
    ListenableFuture<Session> futureSession = createAsyncSessionWithKey();
    Session session = futureSession.get();

    session.execute("SELECT * FROM system_schema.keyspaces;");

    session.closeAsync().get(15, TimeUnit.SECONDS);
    assertTrue(session.isClosed());

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  private void createTable(Session session) {
    session.execute("CREATE TABLE IF NOT EXISTS test.book (id uuid PRIMARY KEY, title text)");
  }

  private void createKeyspace(Session session) {
    PreparedStatement prepared = session.prepare(
        "create keyspace test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
    BoundStatement bound = prepared.bind();
    session.execute(bound);
  }

  private void createKeyspaceAsync(Session session) throws Exception {
    PreparedStatement prepared = session.prepareAsync(
        "create keyspace test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};")
        .get(15, TimeUnit.SECONDS);
    BoundStatement bound = prepared.bind();
    session.executeAsync(bound).get(15, TimeUnit.SECONDS);
  }

  private void createTableAsync(Session session) throws Exception {
    session.executeAsync("CREATE TABLE IF NOT EXISTS test.book (id uuid PRIMARY KEY, title text)")
        .get(15, TimeUnit.SECONDS);
  }

  private void insertAsync(Session session) throws Exception {
    // Insert 1
    session.executeAsync("INSERT INTO test.book (id, title) VALUES (?, ?)",
        UUIDs.timeBased(), "title").get(15, TimeUnit.SECONDS);

    // Insert 2
    Map<String, Object> values = new HashMap<>();
    values.put("id", UUIDs.timeBased());
    values.put("title", "title2");
    session.executeAsync("INSERT INTO test.book (id, title) VALUES (:id, :title)", values)
        .get(15, TimeUnit.SECONDS);

    // Insert 3
    PreparedStatement preparedStatement = session.prepareAsync(
        new SimpleStatement("INSERT INTO test.book (id, title) VALUES (?, ?)"))
        .get(15, TimeUnit.SECONDS);
    session.executeAsync(preparedStatement.bind(UUIDs.timeBased(), "title3"))
        .get(15, TimeUnit.SECONDS);
  }

  private void insert(Session session) {
    // Insert 1
    session.execute("INSERT INTO test.book (id, title) VALUES (?, ?)",
        UUIDs.timeBased(), "title");

    // Insert 2
    Map<String, Object> values = new HashMap<>();
    values.put("id", UUIDs.timeBased());
    values.put("title", "title2");
    session.execute("INSERT INTO test.book (id, title) VALUES (:id, :title)", values);

    // Insert 3
    PreparedStatement preparedStatement = session.prepare(
        new SimpleStatement("INSERT INTO test.book (id, title) VALUES (?, ?)"));
    session.execute(preparedStatement.bind(UUIDs.timeBased(), "title3"));
  }

  private void checkSpans(List<MockSpan> mockSpans) {
    for (MockSpan mockSpan : mockSpans) {
      assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
      assertEquals(TracingSession.COMPONENT_NAME, mockSpan.tags().get(Tags.COMPONENT.getKey()));
      assertEquals("cassandra", mockSpan.tags().get(Tags.DB_TYPE.getKey()));
      assertEquals(0, mockSpan.generatedErrors().size());
      String operationName = mockSpan.operationName();
      assertEquals("execute", operationName);

      assertNotNull(mockSpan.tags().get(Tags.DB_STATEMENT.getKey()));
      assertNotNull(mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));
    }
  }

  private void checkSpanWithError(MockSpan span) {
    assertEquals(Tags.SPAN_KIND_CLIENT, span.tags().get(Tags.SPAN_KIND.getKey()));
    assertEquals(TracingSession.COMPONENT_NAME, span.tags().get(Tags.COMPONENT.getKey()));
    assertEquals("cassandra", span.tags().get(Tags.DB_TYPE.getKey()));
    assertEquals(0, span.generatedErrors().size());
    String operationName = span.operationName();
    assertEquals("execute", operationName);

    assertEquals(true, span.tags().get(Tags.ERROR.getKey()));
    assertFalse(span.logEntries().isEmpty());

    assertNotNull(span.tags().get(Tags.DB_STATEMENT.getKey()));
  }

  private Session createSession() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    Session session = cluster.connect();
    assertFalse(session.isClosed());
    assertNotNull(session.getState());
    assertNotNull(session.getCluster());
    assertNull(session.getLoggedKeyspace());
    return cluster.connect();
  }

  private Session createNewSession() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    return cluster.newSession();
  }

  private Session createSessionWithKey() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    Session session = cluster.connect("system");
    assertEquals("system", session.getLoggedKeyspace());
    assertFalse(session.isClosed());
    assertNotNull(session.getState());
    assertNotNull(session.getCluster());
    return session;
  }

  private ListenableFuture<Session> createAsyncSession() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    return cluster.connectAsync();
  }

  private ListenableFuture<Session> createAsyncSessionWithKey() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    return cluster.connectAsync("system");
  }


}
