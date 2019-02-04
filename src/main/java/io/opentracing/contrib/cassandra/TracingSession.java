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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.cassandra.nameprovider.CustomStringSpanName;
import io.opentracing.contrib.cassandra.nameprovider.QuerySpanNameProvider;
import io.opentracing.tag.Tags;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Package private decorator for {@link Session}
 * Instantiated only by TracingCluster
 */
class TracingSession implements Session {

  static final String COMPONENT_NAME = "java-cassandra";
  private final ExecutorService executorService;
  private final Session session;
  private final Tracer tracer;
  private final QuerySpanNameProvider querySpanNameProvider;

  TracingSession(Session session, Tracer tracer) {
    this.session = session;
    this.tracer = tracer;
    this.querySpanNameProvider = CustomStringSpanName.newBuilder().build("execute");
    this.executorService = Executors.newCachedThreadPool();
  }

  TracingSession(Session session, Tracer tracer, QuerySpanNameProvider querySpanNameProvider) {
    this.session = session;
    this.tracer = tracer;
    this.querySpanNameProvider = querySpanNameProvider;
    this.executorService = Executors.newCachedThreadPool();
  }

  TracingSession(Session session, Tracer tracer, QuerySpanNameProvider querySpanNameProvider, ExecutorService executorService) {
    this.session = session;
    this.tracer = tracer;
    this.querySpanNameProvider = querySpanNameProvider;
    this.executorService = executorService;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public String getLoggedKeyspace() {
    return session.getLoggedKeyspace();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Session init() {
    return new TracingSession(session.init(), tracer, querySpanNameProvider, executorService);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Session> initAsync() {
    return Futures.transform(session.initAsync(), new Function<Session, Session>() {
      @Override
      public Session apply(Session session) {
        return new TracingSession(session, tracer);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSet execute(String query) {
    Span span = buildSpan(query);
    ResultSet resultSet;
    try {
      resultSet = session.execute(query);
      finishSpan(span, resultSet);
      return resultSet;
    } catch (Exception e) {
      finishSpan(span, e);
      throw e;
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSet execute(String query, Object... values) {
    Span span = buildSpan(query);
    ResultSet resultSet;
    try {
      resultSet = session.execute(query, values);
      finishSpan(span, resultSet);
      return resultSet;
    } catch (Exception e) {
      finishSpan(span, e);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSet execute(String query, Map<String, Object> values) {
    Span span = buildSpan(query);
    ResultSet resultSet;
    try {
      resultSet = session.execute(query, values);
      finishSpan(span, resultSet);
      return resultSet;
    } catch (Exception e) {
      finishSpan(span, e);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSet execute(Statement statement) {
    String query = getQuery(statement);
    Span span = buildSpan(query);
    ResultSet resultSet = null;
    try {
      resultSet = session.execute(statement);
      finishSpan(span, resultSet);
      return resultSet;
    } catch (Exception e) {
      finishSpan(span, e);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetFuture executeAsync(String query) {
    final Span span = buildSpan(query);
    ResultSetFuture future = session.executeAsync(query);
    future.addListener(createListener(span, future), executorService);

    return future;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetFuture executeAsync(String query, Object... values) {
    final Span span = buildSpan(query);
    ResultSetFuture future = session.executeAsync(query, values);
    future.addListener(createListener(span, future), executorService);

    return future;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetFuture executeAsync(String query, Map<String, Object> values) {
    final Span span = buildSpan(query);
    ResultSetFuture future = session.executeAsync(query, values);
    future.addListener(createListener(span, future), executorService);

    return future;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetFuture executeAsync(Statement statement) {
    String query = getQuery(statement);
    final Span span = buildSpan(query);
    ResultSetFuture future = session.executeAsync(statement);
    future.addListener(createListener(span, future), executorService);

    return future;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepare(String query) {
    return session.prepare(query);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PreparedStatement prepare(RegularStatement statement) {
    return session.prepare(statement);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(String query) {
    return session.prepareAsync(query);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    return session.prepareAsync(statement);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CloseFuture closeAsync() {
    return session.closeAsync();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    session.close();
  }

  @Override
  public boolean isClosed() {
    return session.isClosed();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Cluster getCluster() {
    return session.getCluster();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public State getState() {
    return session.getState();
  }

  private static String getQuery(Statement statement) {
    String query = null;
    if (statement instanceof BoundStatement) {
      query = ((BoundStatement) statement).preparedStatement().getQueryString();
    } else if (statement instanceof RegularStatement) {
      query = ((RegularStatement) statement).getQueryString();
    }

    return query == null ? "" : query;
  }

  private static Runnable createListener(final Span span, final ResultSetFuture future) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          finishSpan(span, future.get());
        } catch (InterruptedException | ExecutionException e) {
          finishSpan(span, e);
        }
      }
    };
  }

  private Span buildSpan(String query) {
    String querySpanName = querySpanNameProvider.querySpanName(query);
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(querySpanName)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

    Span span = spanBuilder.start();

    Tags.COMPONENT.set(span, COMPONENT_NAME);
    Tags.DB_STATEMENT.set(span, query);
    Tags.DB_TYPE.set(span, "cassandra");

    String keyspace = getLoggedKeyspace();
    if (keyspace != null) {
      Tags.DB_INSTANCE.set(span, keyspace);
    }

    return span;
  }

  private static void finishSpan(Span span, ResultSet resultSet) {
    if (resultSet != null) {
      Host host = resultSet.getExecutionInfo().getQueriedHost();
      Tags.PEER_PORT.set(span, host.getSocketAddress().getPort());

      Tags.PEER_HOSTNAME.set(span, host.getAddress().getHostName());
      InetAddress inetAddress = host.getSocketAddress().getAddress();

      if (inetAddress instanceof Inet4Address) {
        byte[] address = inetAddress.getAddress();
        Tags.PEER_HOST_IPV4.set(span, ByteBuffer.wrap(address).getInt());
      } else {
        Tags.PEER_HOST_IPV6.set(span, inetAddress.getHostAddress());
      }

    }
    span.finish();
  }

  private static void finishSpan(Span span, Exception e) {
    Tags.ERROR.set(span, Boolean.TRUE);
    span.log(errorLogs(e));
    span.finish();
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(4);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.kind", throwable.getClass().getName());
    errorLogs.put("error.object", throwable);

    errorLogs.put("message", throwable.getMessage());

    StringWriter sw = new StringWriter();
    throwable.printStackTrace(new PrintWriter(sw));
    errorLogs.put("stack", sw.toString());

    return errorLogs;
  }
}
