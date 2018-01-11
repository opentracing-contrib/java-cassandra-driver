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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentracing.Scope;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.ThreadLocalScopeManager;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraTest {

  private static final MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);

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
  public void withoutParent() throws Exception {
    Session session = createSession();

    ResultSetFuture future = session.executeAsync("SELECT * FROM system_schema.keyspaces;");

    ResultSet resultSet = future.get();
    assertNotNull(resultSet.one());

    PreparedStatement prepared = session.prepare(
        "create keyspace test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
    BoundStatement bound = prepared.bind();
    session.execute(bound);

    session.execute("USE test;");
    session.execute("SELECT * FROM system_schema.keyspaces;");

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(4, finished.size());

    for (MockSpan mockSpan : finished) {
      assertEquals(0, mockSpan.parentId());
    }

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void withParent() throws Exception {
    Session session = createSession();

    Scope parent = mockTracer.buildSpan("parent").startActive(true);

    session.executeAsync("SELECT * FROM system_schema.keyspaces;").get();
    session.execute("SELECT * FROM system_schema.keyspaces;");

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(2));

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(2, finished.size());
    for (MockSpan mockSpan : finished) {
      assertEquals(((MockSpan) parent.span()).context().spanId(), mockSpan.parentId());
      assertEquals(((MockSpan) parent.span()).context().traceId(), mockSpan.context().traceId());
    }

    checkSpans(finished);
    assertNotNull(mockTracer.activeSpan());
  }

  @Test
  public void usingNewSession() {
    Session session = createNewSession();

    session.execute("SELECT * FROM system_schema.keyspaces;");

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void connectAsync() throws ExecutionException, InterruptedException {
    ListenableFuture<Session> futureSession = createAsincSession();
    Session session = futureSession.get();

    session.execute("SELECT * FROM system_schema.keyspaces;");

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void connectAsyncWithKey() throws ExecutionException, InterruptedException {
    ListenableFuture<Session> futureSession = createAsincSessionWithKey();
    Session session = futureSession.get();

    session.execute("SELECT * FROM system_schema.keyspaces;");

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());

    checkSpans(finished);
    assertNull(mockTracer.activeSpan());
  }

  private void checkSpans(List<MockSpan> mockSpans) {
    for (MockSpan mockSpan : mockSpans) {
      assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
      assertEquals(TracingSession.COMPONENT_NAME, mockSpan.tags().get(Tags.COMPONENT.getKey()));
      assertEquals("cassandra", mockSpan.tags().get(Tags.DB_TYPE.getKey()));
      assertEquals(0, mockSpan.generatedErrors().size());
      String operationName = mockSpan.operationName();
      assertTrue(operationName.equals("execute"));

      assertNotNull(mockSpan.tags().get(Tags.DB_STATEMENT.getKey()));
      assertNotNull(mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));
    }
  }

  private Session createSession() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    return cluster.connect();
  }

  private Session createNewSession() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    return cluster.newSession();
  }

  private ListenableFuture<Session> createAsincSession() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    return cluster.connectAsync();
  }

  private ListenableFuture<Session> createAsincSessionWithKey() {
    Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);
    Cluster cluster = new TracingCluster(builder, mockTracer);
    return cluster.connectAsync("system");
  }

  private Callable<Integer> reportedSpansSize() {
    return new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return mockTracer.finishedSpans().size();
      }
    };
  }
}
