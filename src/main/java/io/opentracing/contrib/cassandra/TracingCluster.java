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
package io.opentracing.contrib.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.GuavaCompatibility;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentracing.Tracer;
import io.opentracing.contrib.cassandra.nameprovider.CustomStringSpanName;
import io.opentracing.contrib.cassandra.nameprovider.QuerySpanNameProvider;
import io.opentracing.util.GlobalTracer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tracing decorator for {@link Cluster}
 */
public class TracingCluster extends Cluster {

  private final Tracer tracer;
  private final QuerySpanNameProvider querySpanNameProvider;
  private final ExecutorService executorService;

  public TracingCluster(Initializer initializer, Tracer tracer) {
    this(initializer, tracer, CustomStringSpanName.newBuilder().build("execute"));
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingCluster(Initializer initializer) {
    this(initializer, GlobalTracer.get());
  }

  public TracingCluster(Initializer initializer, Tracer tracer,
      QuerySpanNameProvider querySpanNameProvider) {
    this(initializer, tracer, querySpanNameProvider, Executors.newCachedThreadPool());
  }

  public TracingCluster(Initializer initializer, Tracer tracer,
      QuerySpanNameProvider querySpanNameProvider,
      ExecutorService executorService) {
    super(initializer);
    this.tracer = tracer;
    this.querySpanNameProvider = querySpanNameProvider;
    this.executorService = executorService;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingCluster(Initializer initializer, QuerySpanNameProvider querySpanNameProvider) {
    this(initializer, GlobalTracer.get(), querySpanNameProvider);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Session newSession() {
    return new TracingSession(super.newSession(), tracer, querySpanNameProvider, executorService);
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
    return GuavaCompatibility.INSTANCE.transform(super.connectAsync(keyspace), new Function<Session, Session>() {
      @Override
      public Session apply(Session session) {
        return new TracingSession(session, tracer, querySpanNameProvider, executorService);
      }
    });
  }
}
