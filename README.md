[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven]

# OpenTracing Cassandra Driver Instrumentation
OpenTracing instrumentation for Cassandra Driver.

## Installation

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-cassandra-driver</artifactId>
    <version>VERSION</version>
</dependency>
```

## Usage

```java
// Instantiate tracer
Tracer tracer = ...

// Instantiate Cluster Builder
 Cluster.Builder builder = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142);

// Instantiate Tracing Cluster
Cluster cluster = new TracingCluster(builder, tracer);

```

[ci-img]: https://travis-ci.org/opentracing-contrib/java-cassandra-driver.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-cassandra-driver
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-cassandra-driver/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-cassandra-driver?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-cassandra-driver.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-cassandra-driver

