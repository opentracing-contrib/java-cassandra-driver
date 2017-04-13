[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]

# OpenTracing Cassandra Driver Instrumentation
OpenTracing instrumentation for Cassandra Driver.

## Installation

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-cassandra-driver</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Usage

`DefaultSpanManager` is used to get active span

```java
// Instantiate tracer
Tracer tracer = ...

// Register tracer with GlobalTracer
GlobalTracer.register(tracer);

// Instantiate Cluster Builder
 Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);

// Instantiate Tracing Cluster
Cluster cluster = new TracingCluster(builder);

```

[ci-img]: https://travis-ci.org/opentracing-contrib/java-cassandra-driver.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-cassandra-driver
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-cassandra-driver.svg?maxAge=2592000
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-cassandra-driver

