package io.opentracing.contrib.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentracing.Tracer;

/**
 * Tracing decorator for {@link Cluster}
 */
public class TracingCluster extends Cluster {

  private final Tracer tracer;

  public TracingCluster(Initializer initializer, Tracer tracer) {
    super(initializer);
    this.tracer = tracer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Session newSession() {
    return new TracingSession(super.newSession(), tracer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Session connect() {
    // result shouldn't be wrapped into TracingSession because it internally calls connectAsync(String keyspace)
    return super.connect();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Session connect(String keyspace) {
    // result shouldn't be wrapped into TracingSession because it internally calls connectAsync(String keyspace)
    return super.connect(keyspace);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Session> connectAsync() {
    // result shouldn't be wrapped into TracingSession because it internally calls connectAsync(String keyspace)
    return super.connectAsync();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ListenableFuture<Session> connectAsync(String keyspace) {
    return Futures.transform(super.connectAsync(keyspace), new Function<Session, Session>() {
      @Override
      public Session apply(Session session) {
        return new TracingSession(session, tracer);
      }
    });
  }
}
