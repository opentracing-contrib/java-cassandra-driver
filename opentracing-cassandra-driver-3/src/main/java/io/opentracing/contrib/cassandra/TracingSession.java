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
package io.opentracing.contrib.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.GuavaCompatibility;
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
import io.opentracing.tag.BooleanTag;
import io.opentracing.tag.IntTag;
import io.opentracing.tag.StringTag;
import io.opentracing.tag.Tags;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Decorator for {@link Session} Instantiated by TracingCluster
 */
public class TracingSession implements Session {

  static final String COMPONENT_NAME = "java-cassandra";
  static final StringTag QUERY_CONSISTENCY_LEVEL = new StringTag("query.cl");
  static final StringTag QUERY_TABLE = new StringTag("query.table");
  static final IntTag QUERY_FETCH_SIZE = new IntTag("query.fetchSize");
  static final BooleanTag QUERY_IDEMPOTENCE = new BooleanTag("query.idempotence");

  private final Executor executor;
  private final Session session;
  private final Tracer tracer;
  private final QuerySpanNameProvider querySpanNameProvider;

  public TracingSession(Session session, Tracer tracer) {
    this(session, tracer, CustomStringSpanName.newBuilder().build("execute"));
  }

  public TracingSession(Session session, Tracer tracer,
      QuerySpanNameProvider querySpanNameProvider) {
    this(session, tracer, querySpanNameProvider, Executors.newCachedThreadPool());
  }

  public TracingSession(Session session, Tracer tracer, QuerySpanNameProvider querySpanNameProvider,
      Executor executor) {
    this.session = session;
    this.tracer = tracer;
    this.querySpanNameProvider = querySpanNameProvider;
    this.executor = executor;
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
    return new TracingSession(session.init(), tracer, querySpanNameProvider, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Session> initAsync() {
    if (GuavaCompatibilityUtil.isGuavaCompatibilityFound()) {
      return GuavaCompatibility.INSTANCE
          .transform(session.initAsync(), new Function<Session, Session>() {
            @Override
            public Session apply(Session session) {
              return new TracingSession(session, tracer);
            }
          });
    } else {
      return Futures.transform(session.initAsync(), new Function<Session, Session>() {
        @Override
        public Session apply(Session session) {
          return new TracingSession(session, tracer);
        }
      });
    }
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
      finishSpan(span, resultSet, statement);
      return resultSet;
    } catch (Exception e) {
      finishSpan(span, e, statement);
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
    future.addListener(createListener(span, future), executor);

    return future;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetFuture executeAsync(String query, Object... values) {
    final Span span = buildSpan(query);
    ResultSetFuture future = session.executeAsync(query, values);
    future.addListener(createListener(span, future), executor);

    return future;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetFuture executeAsync(String query, Map<String, Object> values) {
    final Span span = buildSpan(query);
    ResultSetFuture future = session.executeAsync(query, values);
    future.addListener(createListener(span, future), executor);

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
    future.addListener(createListener(span, future, statement), executor);

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

  private Runnable createListener(final Span span, final ResultSetFuture future) {
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

  private Runnable createListener(final Span span,
      final ResultSetFuture future,
      final Statement statement) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          finishSpan(span, future.get(), statement);
        } catch (InterruptedException | ExecutionException e) {
          finishSpan(span, e, statement);
        }
      }
    };
  }


  /**
   * Build span for distributed tracing. Method can be overridden by subclasses to add custom tags.
   *
   * @param query cql query statement
   * @return OpenTracing Span
   */
  public Span buildSpan(String query) {
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

  /**
   * Add OpenTracing tags after executing the query. Method can be overridden by subclasses to add
   * custom tags.
   *
   * @param span OpenTracing Span
   * @param resultSet ResultSet returns from executing the query
   */
  public void finishSpan(Span span, ResultSet resultSet) {
    addDefaultTags(span);

    if (resultSet != null) {
      Host host = resultSet.getExecutionInfo().getQueriedHost();
      Tags.PEER_PORT.set(span, host.getSocketAddress().getPort());

      Tags.PEER_HOSTNAME.set(span, host.getAddress().getHostName());
      InetAddress inetAddress = host.getSocketAddress().getAddress();

      if (inetAddress instanceof Inet4Address) {
        Tags.PEER_HOST_IPV4.set(span, inetAddress.getHostAddress());
      } else {
        Tags.PEER_HOST_IPV6.set(span, inetAddress.getHostAddress());
      }
    }
    span.finish();
  }

  /**
   * Add OpenTracing tags after executing the query. Method can be overridden by subclasses to add
   * custom tags.
   *
   * @param span OpenTracing Span
   * @param resultSet ResultSet returns from executing the query
   * @param statement Query statement
   */
  public void finishSpan(Span span, ResultSet resultSet, Statement statement) {
    addDefaultStatementTags(span, statement);

    if (resultSet != null) {
      Host host = resultSet.getExecutionInfo().getQueriedHost();
      InetSocketAddress hostSocket = host.getEndPoint().resolve();
      Tags.PEER_PORT.set(span, hostSocket.getPort());
      Tags.PEER_HOSTNAME.set(span, hostSocket.getHostName());
      InetAddress inetAddress = hostSocket.getAddress();
      if (inetAddress instanceof Inet4Address) {
        Tags.PEER_HOST_IPV4.set(span, inetAddress.getHostAddress());
      } else {
        Tags.PEER_HOST_IPV6.set(span, inetAddress.getHostAddress());
      }
    }
    span.finish();
  }

  /**
   * Add OpenTracing tags after executing the query. Method can be overridden by subclasses to add
   * custom tags.
   *
   * @param span OpenTracing Span
   * @param e Exception thrown while executing the query
   * @param statement Query statement
   */
  public void finishSpan(Span span, Exception e, Statement statement) {
    Tags.ERROR.set(span, Boolean.TRUE);

    addDefaultStatementTags(span, statement);

    if (statement instanceof BoundStatement) {
      span.log(errorLogs(e, statement));
    } else {
      span.log(errorLogs(e));
    }
    span.finish();
  }

  /**
   * Add OpenTracing tags after executing the query. Method can be overridden by subclasses to add
   * custom tags.
   *
   * @param span OpenTracing Span
   * @param e Exception thrown while executing the query
   */
  public void finishSpan(Span span, Exception e) {
    Tags.ERROR.set(span, Boolean.TRUE);

    addDefaultTags(span);

    span.log(errorLogs(e));
    span.finish();
  }

  /**
   * Add error logs. Method can be overridden by subclasses to add custom error logs.
   *
   * @param throwable Exception thrown while executing the query
   * @return Error Logs
   */
  public Map<String, Object> errorLogs(Throwable throwable) {
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

  /**
   * Add error logs. Method can be overridden by subclasses to add custom error logs.
   *
   * @param throwable Exception thrown while excuting the query
   * @param statement Query statement
   * @return Error Logs
   */
  public Map<String, Object> errorLogs(Throwable throwable, Statement statement) {
    Map<String, Object> errorLogs = errorLogs(throwable);
    String statementStr = statement.toString();
    errorLogs.put("query.statement", statementStr);
    return errorLogs;
  }

  private void addDefaultStatementTags(Span span, Statement statement) {
    ConsistencyLevel cl = statement.getConsistencyLevel();
    if (cl == null && session.getCluster().getConfiguration().getQueryOptions()
        .isConsistencySet()) {
      cl = session.getCluster().getConfiguration().getQueryOptions().getConsistencyLevel();
    }
    if (cl != null) {
      QUERY_CONSISTENCY_LEVEL.set(span, cl.name());
    }

    int fetchSize = statement.getFetchSize();
    if (fetchSize == 0) {
      fetchSize = session.getCluster().getConfiguration().getQueryOptions().getFetchSize();
    }
    QUERY_FETCH_SIZE.set(span, fetchSize);

    boolean isIdempotent = session.getCluster().getConfiguration().getQueryOptions()
        .getDefaultIdempotence();
    if (statement.isIdempotent() != null) {
      isIdempotent = statement.isIdempotent();
    }
    QUERY_IDEMPOTENCE.set(span, isIdempotent);
  }

  private void addDefaultTags(Span span) {
    ConsistencyLevel cl = session.getCluster().getConfiguration().getQueryOptions()
        .getConsistencyLevel();
    if (cl != null) {
      QUERY_CONSISTENCY_LEVEL.set(span, cl.name());
    }

    int fetchSize = session.getCluster().getConfiguration().getQueryOptions().getFetchSize();

    QUERY_FETCH_SIZE.set(span, fetchSize);

    boolean isIdempotent = session.getCluster().getConfiguration().getQueryOptions()
        .getDefaultIdempotence();
    QUERY_IDEMPOTENCE.set(span, isIdempotent);
  }
}
