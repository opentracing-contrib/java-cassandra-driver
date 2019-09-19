[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven] [![Apache-2.0 license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# OpenTracing Cassandra Driver Instrumentation
OpenTracing instrumentation for Cassandra Driver.

## Installation

### Cassandra 3
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-cassandra-driver-3</artifactId>
    <version>VERSION</version>
</dependency>
```

### Cassandra 4
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-cassandra-driver-4</artifactId>
    <version>VERSION</version>
</dependency>
```

## Usage

```java
// Instantiate tracer
Tracer tracer = ...
```

### Cassandra 3
```java
// Instantiate Cluster Builder:
 Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);

// Instantiate Tracing Cluster:
Cluster cluster = new TracingCluster(builder, tracer);
```

### Cassandra 4
```java
// Instantiate CqlSession:
CqlSession session = CqlSession.builder().build()

// Decorate CqlSession with TracingCqlSession:
CqlSession tracingSession = new TracingCqlSession(session, tracer);

// execute query with TracingCqlSession:
tracingSession.execute("...");

```

## Span Names for Cassandra 3
By default, spans for executed queries will be created with the name `execute`.
To use a different name for the query spans, you can create a custom name provider by implementing
the QuerySpanNameProvider interface.

### QuerySpanNameProvider interface

```java
public interface QuerySpanNameProvider {
  public interface Builder {
    QuerySpanNameProvider build();
  }

  /**
   * Given a Cassandra query, return a name for the span
   * @param query Cassandra query
   * @return Name for query's span
   */
  String querySpanName(String query);

}
```

### CustomStringSpanName
Returns a predefined string for every span. Defaults to `execute` on null or "" argument to build()
```java
import io.opentracing.contrib.cassandra.QuerySpanNameProvider.CustomStringSpanName;
import io.opentracing.contrib.cassandra.QuerySpanNameProvider.QuerySpanNameProvider;
...

// Initialize with custom string
QuerySpanNameProvider querySpanNameProvider = CustomStringSpanName.newBuilder().build("CUSTOM_NAME");


// Instantiate Tracing Cluster with QuerySpanNameProvider as an argument
Tracer tracer = ...
Cluster.Builder builder = ...
Cluster cluster = new TracingCluster(builder, tracer, querySpanNameProvider);

Session session = cluster.newSession();

// Execute query
session.execute("SELECT * FROM example.table WHERE field = ?", "test");
// Span is created with span name 
// "CUSTOM_NAME"

```

### FullQuerySpanName
Returns the full query as the span name.
```java
import io.opentracing.contrib.cassandra.QuerySpanNameProvider.FullQuerySpanName;
import io.opentracing.contrib.cassandra.QuerySpanNameProvider.QuerySpanNameProvider;
...

// Initialize
QuerySpanNameProvider querySpanNameProvider = FullQuerySpanName.newBuilder().build();


// Instantiate Tracing Cluster with QuerySpanNameProvider as an argument
Tracer tracer = ...
Cluster.Builder builder = ...
Cluster cluster = new TracingCluster(builder, tracer, querySpanNameProvider);

Session session = cluster.newSession();

// Execute query
session.execute("SELECT * FROM example.table WHERE field = ?", "test");

// Span is created with the full query as the span name,
// "SELECT * FROM example.table WHERE field = ?;"

// Span name will be parameterized if the original given query is parameterized.

```

### PrefixedFullQuerySpanName
Returns the full query as the span name, with a custom string prefix. Defaults to `Cassandra` on null or "" argument to build().
```java
import io.opentracing.contrib.cassandra.QuerySpanNameProvider.PrefixedFullQuerySpanName;
import io.opentracing.contrib.cassandra.QuerySpanNameProvider.QuerySpanNameProvider;
...

// Initialize with custom prefix string
QuerySpanNameProvider querySpanNameProvider = PrefixedFullQuerySpanName.newBuilder().build("CUSTOM_PREFIX");


// Instantiate Tracing Cluster with QuerySpanNameProvider as an argument
Tracer tracer = ...
Cluster.Builder builder = ...
Cluster cluster = new TracingCluster(builder, tracer, querySpanNameProvider);


Session session = cluster.newSession();

// Execute query
session.execute("SELECT * FROM example.table WHERE field = ?", "test");

// Span is created with the full query as the span name, prefixed with the custom prefix, 
// "CUSTOM_PREFIX: SELECT * FROM example.table WHERE field = ?;"

// Span name will be parameterized if the original given query is parameterized.

```

### QueryMethodTableSpanName
Returns formatted string `Cassandra.[METHOD] - [TARGET_ENTITY]` where [METHOD] is the Cassandra Method and [TARGET_ENTITY] is the
entity that the method is acting on (view, keyspace, table, index). 

For methods that require no target entity, returns `Cassandra.[METHOD]`.


If a method does require a target entity, but none is found, returns `Cassandra.[METHOD] - N/A`.


The supported Cassandra methods are:
- SELECT
- INSERT
- UPDATE
- DELETE
- BATCH
- USE
- CREATE MATERIALIZED VIEW
- ALTER MATERIALIZED VIEW
- DROP MATERIALIZED VIEW
- CREATE KEYSPACE
- ALTER KEYSPACE
- DROP KEYSPACE
- CREATE TABLE
- ALTER TABLE
- DROP TABLE
- TRUNCATE
- CREATE INDEX
- DROP INDEX
```java
import io.opentracing.contrib.cassandra.QuerySpanNameProvider.QueryMethodTableSpanName;
import io.opentracing.contrib.cassandra.QuerySpanNameProvider.QuerySpanNameProvider;
...

// Initialize
QuerySpanNameProvider querySpanNameProvider = QueryMethodTableSpanName.newBuilder().build();


// Instantiate Tracing Cluster with QuerySpanNameProvider as an argument
Tracer tracer = ...
Cluster.Builder builder = ...
Cluster cluster = new TracingCluster(builder, tracer, querySpanNameProvider);

Session session = cluster.newSession();

// Execute query
session.execute("SELECT * FROM example.table WHERE field = ?", "test");

// Span is created with the method and target entity in the name,
// "Cassandra.SELECT - example.table"

// Provider has support for additional qualifiers IF EXISTS and IF NOT EXISTS
session.execute("CREATE TABLE example.table;");
session.execute("CREATE TABLE IF NOT EXISTS example.table;")

// Two spans are created with the same name,
// "Cassandra.CREATE_TABLE - example.table"

```

## License

[Apache 2.0 License](./LICENSE).

[ci-img]: https://travis-ci.org/opentracing-contrib/java-cassandra-driver.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-cassandra-driver
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-cassandra-driver/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-cassandra-driver?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-cassandra-driver.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-cassandra-driver

