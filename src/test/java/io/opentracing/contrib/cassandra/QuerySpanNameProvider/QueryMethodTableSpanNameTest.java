package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class QueryMethodTableSpanNameTest {

  @Test
  public void manipulationMethodTest() {
  QuerySpanNameProvider querySpanNameProvider = QueryMethodTableSpanName.newBuilder().build();
  // SELECT
  assertEquals("Cassandra.SELECT - test.table_name", querySpanNameProvider.querySpanName("SELECT * FROM test.table_name;"));
  assertEquals("Cassandra.SELECT - N/A", querySpanNameProvider.querySpanName("SELECT *;"));
  assertEquals("Cassandra.SELECT - N/A", querySpanNameProvider.querySpanName("SELECT * FROM ;"));
  // INSERT
  assertEquals("Cassandra.INSERT - test.table_name", querySpanNameProvider.querySpanName("INSERT INTO test.table_name;"));
  assertEquals("Cassandra.INSERT - N/A", querySpanNameProvider.querySpanName("INSERT;"));
  assertEquals("Cassandra.INSERT - N/A", querySpanNameProvider.querySpanName("INSERT INTO ;"));
  // UPDATE
  assertEquals("Cassandra.UPDATE - test.table_name", querySpanNameProvider.querySpanName("UPDATE test.table_name;"));
  assertEquals("Cassandra.UPDATE - N/A", querySpanNameProvider.querySpanName("UPDATE;"));
  assertEquals("Cassandra.UPDATE - N/A", querySpanNameProvider.querySpanName("UPDATE ;"));
  // DELETE
  assertEquals("Cassandra.DELETE - test.table_name", querySpanNameProvider.querySpanName("DELETE * FROM test.table_name;"));
  assertEquals("Cassandra.DELETE - N/A", querySpanNameProvider.querySpanName("DELETE *;"));
  assertEquals("Cassandra.DELETE - N/A", querySpanNameProvider.querySpanName("DELETE * FROM ;"));
  // BATCH
  assertEquals("Cassandra.BATCH", querySpanNameProvider.querySpanName("BEGIN BATCH;"));
  assertEquals("Cassandra.BATCH", querySpanNameProvider.querySpanName("APPLY BATCH;"));
  }

  @Test
  public void definitionMethodTest() {
  QuerySpanNameProvider querySpanNameProvider = new QueryMethodTableSpanName().newBuilder().build();
  // USE
  assertEquals("Cassandra.USE - test", querySpanNameProvider.querySpanName("USE test;"));
  assertEquals("Cassandra.USE - N/A", querySpanNameProvider.querySpanName("USE;"));
  assertEquals("Cassandra.USE - N/A", querySpanNameProvider.querySpanName("USE ;"));
  // CREATE_KEYSPACE
  assertEquals("Cassandra.CREATE_KEYSPACE - test", querySpanNameProvider.querySpanName("CREATE KEYSPACE test;"));
  assertEquals("Cassandra.CREATE_KEYSPACE - test", querySpanNameProvider.querySpanName("CREATE KEYSPACE IF NOT EXISTS test;"));
  assertEquals("Cassandra.CREATE_KEYSPACE - N/A", querySpanNameProvider.querySpanName("CREATE KEYSPACE"));
  assertEquals("Cassandra.CREATE_KEYSPACE - N/A", querySpanNameProvider.querySpanName("CREATE KEYSPACE ;"));
  // ALTER_KEYSPACE
  assertEquals("Cassandra.ALTER_KEYSPACE - test", querySpanNameProvider.querySpanName("ALTER KEYSPACE test;"));
  assertEquals("Cassandra.ALTER_KEYSPACE - N/A", querySpanNameProvider.querySpanName("ALTER KEYSPACE;"));
  assertEquals("Cassandra.ALTER_KEYSPACE - N/A", querySpanNameProvider.querySpanName("ALTER KEYSPACE ;"));
  // DROP_KEYSPACE
  assertEquals("Cassandra.DROP_KEYSPACE - test", querySpanNameProvider.querySpanName("DROP KEYSPACE test;"));
  assertEquals("Cassandra.DROP_KEYSPACE - test", querySpanNameProvider.querySpanName("DROP KEYSPACE IF EXISTS test;"));
  assertEquals("Cassandra.DROP_KEYSPACE - N/A", querySpanNameProvider.querySpanName("DROP KEYSPACE;"));
  assertEquals("Cassandra.DROP_KEYSPACE - N/A", querySpanNameProvider.querySpanName("DROP KEYSPACE ;"));
  // CREATE_TABLE
  assertEquals("Cassandra.CREATE_TABLE - test.table_name", querySpanNameProvider.querySpanName("CREATE TABLE test.table_name;"));
  assertEquals("Cassandra.CREATE_TABLE - test.table_name", querySpanNameProvider.querySpanName("CREATE TABLE IF NOT EXISTS test.table_name;"));
  assertEquals("Cassandra.CREATE_TABLE - N/A", querySpanNameProvider.querySpanName("CREATE TABLE;"));
  assertEquals("Cassandra.CREATE_TABLE - N/A", querySpanNameProvider.querySpanName("CREATE TABLE ;"));
  // ALTER_TABLE
  assertEquals("Cassandra.ALTER_TABLE - test.table_name", querySpanNameProvider.querySpanName("ALTER TABLE test.table_name;"));
  assertEquals("Cassandra.ALTER_TABLE - N/A", querySpanNameProvider.querySpanName("ALTER TABLE;"));
  assertEquals("Cassandra.ALTER_TABLE - N/A", querySpanNameProvider.querySpanName("ALTER TABLE ;"));
  // DROP_TABLE
  assertEquals("Cassandra.DROP_TABLE - test.table_name", querySpanNameProvider.querySpanName("DROP TABLE test.table_name;"));
  assertEquals("Cassandra.DROP_TABLE - test.table_name", querySpanNameProvider.querySpanName("DROP TABLE IF EXISTS test.table_name;"));
  assertEquals("Cassandra.DROP_TABLE - N/A", querySpanNameProvider.querySpanName("DROP TABLE;"));
  assertEquals("Cassandra.DROP_TABLE - N/A", querySpanNameProvider.querySpanName("DROP TABLE ;"));
  // TRUNCATE
  assertEquals("Cassandra.TRUNCATE - test.table_name", querySpanNameProvider.querySpanName("TRUNCATE test.table_name;"));
  assertEquals("Cassandra.TRUNCATE - test.table_name", querySpanNameProvider.querySpanName("TRUNCATE TABLE test.table_name;"));
  assertEquals("Cassandra.TRUNCATE - N/A", querySpanNameProvider.querySpanName("TRUNCATE;"));
  assertEquals("Cassandra.TRUNCATE - N/A", querySpanNameProvider.querySpanName("TRUNCATE ;"));
  // CREATE_INDEX
  assertEquals("Cassandra.CREATE_INDEX - test_index", querySpanNameProvider.querySpanName("CREATE INDEX test_index;"));
  assertEquals("Cassandra.CREATE_INDEX - test_index", querySpanNameProvider.querySpanName("CREATE INDEX IF NOT EXISTS test_index;"));
  assertEquals("Cassandra.CREATE_INDEX - N/A", querySpanNameProvider.querySpanName("CREATE INDEX;"));
  assertEquals("Cassandra.CREATE_INDEX - N/A", querySpanNameProvider.querySpanName("CREATE INDEX ;"));
  // DROP_INDEX
  assertEquals("Cassandra.DROP_INDEX - test_index", querySpanNameProvider.querySpanName("DROP INDEX test_index;"));
  assertEquals("Cassandra.DROP_INDEX - test_index", querySpanNameProvider.querySpanName("DROP INDEX IF EXISTS test_index;"));
  assertEquals("Cassandra.DROP_INDEX - N/A", querySpanNameProvider.querySpanName("DROP INDEX;"));
  assertEquals("Cassandra.DROP_INDEX - N/A", querySpanNameProvider.querySpanName("DROP INDEX ;"));
  // CREATE_MATERIALIZED_VIEW
  assertEquals("Cassandra.CREATE_MATERIALIZED_VIEW - view_name", querySpanNameProvider.querySpanName("CREATE MATERIALIZED VIEW view_name;"));
  assertEquals("Cassandra.CREATE_MATERIALIZED_VIEW - view_name", querySpanNameProvider.querySpanName("CREATE MATERIALIZED VIEW IF NOT EXISTS view_name;"));
  assertEquals("Cassandra.CREATE_MATERIALIZED_VIEW - N/A", querySpanNameProvider.querySpanName("CREATE MATERIALIZED VIEW;"));
  assertEquals("Cassandra.CREATE_MATERIALIZED_VIEW - N/A", querySpanNameProvider.querySpanName("CREATE MATERIALIZED VIEW ;"));
  // ALTER_MATERIALIZED_VIEW
  assertEquals("Cassandra.ALTER_MATERIALIZED_VIEW - view_name", querySpanNameProvider.querySpanName("ALTER MATERIALIZED VIEW view_name;"));
  assertEquals("Cassandra.ALTER_MATERIALIZED_VIEW - N/A", querySpanNameProvider.querySpanName("ALTER MATERIALIZED VIEW;"));
  assertEquals("Cassandra.ALTER_MATERIALIZED_VIEW - N/A", querySpanNameProvider.querySpanName("ALTER MATERIALIZED VIEW ;"));
  // DROP_MATERIALIZED_VIEW
  assertEquals("Cassandra.DROP_MATERIALIZED_VIEW - view_name", querySpanNameProvider.querySpanName("DROP MATERIALIZED VIEW view_name;"));
  assertEquals("Cassandra.DROP_MATERIALIZED_VIEW - view_name", querySpanNameProvider.querySpanName("DROP MATERIALIZED VIEW IF EXISTS view_name;"));
  assertEquals("Cassandra.DROP_MATERIALIZED_VIEW - N/A", querySpanNameProvider.querySpanName("DROP MATERIALIZED VIEW;"));
  assertEquals("Cassandra.DROP_MATERIALIZED_VIEW - N/A", querySpanNameProvider.querySpanName("DROP MATERIALIZED VIEW ;"));
  }

  @Test
  public void testInvalaidMethod() {
  QuerySpanNameProvider querySpanNameProvider = new QueryMethodTableSpanName().newBuilder().build();
  assertEquals("Cassandra", querySpanNameProvider.querySpanName(""));
  assertEquals("Cassandra", querySpanNameProvider.querySpanName(null));
  assertEquals("Cassandra", querySpanNameProvider.querySpanName("INVALID METHOD"));
  }
}
