package io.opentracing.contrib.cassandra.QuerySpanNameProvider;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryMethodTableSpanName implements QuerySpanNameProvider {

  // Pulled from http://cassandra.apache.org/doc/latest/cql/
  enum ManipulationMethod
  {
    SELECT, INSERT, UPDATE, DELETE, BATCH, NOT_FOUND
  }

  private final String[] CASSANDRA_MANIPULATION_METHODS = {
      "BATCH",
      "SELECT",
      "INSERT",
      "UPDATE",
      "DELETE"
  };

  // Pulled from http://cassandra.apache.org/doc/latest/cql/
  enum DefinitionMethod
  {
    CREATE_KEYSPACE, USE, ALTER_KEYSPACE, DROP_KEYSPACE, CREATE_TABLE,
    ALTER_TABLE, DROP_TABLE, TRUNCATE, CREATE_INDEX, DROP_INDEX,
    CREATE_MATERIALIZED_VIEW, ALTER_MATERIALIZED_VIEW,
    DROP_MATERIALIZED_VIEW, NOT_FOUND
  }

  private final String[] CASSANDRA_DEFINITION_METHODS = {
      "CREATE KEYSPACE",
      "USE",
      "ALTER KEYSPACE",
      "DROP KEYSPACE",
      "CREATE TABLE",
      "ALTER TABLE",
      "DROP TABLE",
      "TRUNCATE",
      "CREATE INDEX",
      "DROP INDEX",
      "CREATE MATERIALIZED VIEW",
      "ALTER MATERIALIZED VIEW",
      "DROP MATERIALIZED VIEW"
  };

  static class Builder implements QuerySpanNameProvider.Builder {
    @Override
    public QuerySpanNameProvider build() { return new io.opentracing.contrib.cassandra.QuerySpanNameProvider.QueryMethodTableSpanName();}

  }

  QueryMethodTableSpanName() {
  }

  @Override
  public String querySpanName(String query) {
    // Short Circuit
    if(query == null || query.equals("")) {
      return "Cassandra";
    }

    String queryMethod;
    String queryTable = "N/A";
    // First check to see if query is a manipulation method
    ManipulationMethod manipulationMethod = getManipulationMethod(query);
    queryMethod = manipulationMethod.toString();
    if(!manipulationMethod.equals(ManipulationMethod.NOT_FOUND)) {
      switch (manipulationMethod) {
        case SELECT:    // SELECT ... FROM X ...
        case DELETE:    // DELETE ... FROM X ...
          queryTable = findTableAfter(query, "FROM");
          break;
        case INSERT:    // INSERT ... INTO X ...
          queryTable = findTableAfter(query, "INTO");
          break;
        case UPDATE:    // UPDATE X ...
          queryTable = findTableAfter(query, "UPDATE");
          break;
        case BATCH:     // BATCH
          return String.format("Cassandra.%s",queryMethod);
        default:
          return "Cassandra";
      }
      return String.format("Cassandra.%s - %s", manipulationMethod.toString(), queryTable);
    }

    // If reached this point, manipulationMethod is set to  NOT_FOUND
    DefinitionMethod definitionMethod = getDefininitionMethod(query);
    queryMethod = definitionMethod.toString().replace("_", " ");
    if(!definitionMethod.equals(DefinitionMethod.NOT_FOUND)) {
      switch(definitionMethod) {
        case USE:             // USE X ...
        case ALTER_KEYSPACE:      // ALTER KEYSPACE X ...
        case ALTER_MATERIALIZED_VIEW:   // ALTER MATERIALIZED VIEW X ...
        case ALTER_TABLE:         // ALTER TABLE X ...
          queryTable = findTableAfter(query, queryMethod);
          break;
        case DROP_INDEX:        // DROP INDEX [IF EXISTS] X ...
        case DROP_KEYSPACE:       // DROP KEYSPACE [IF EXISTS] X ...
        case DROP_MATERIALIZED_VIEW:  // DROP MATERIALIZED VIEW [IF EXISTS] X ...
        case DROP_TABLE:        // DROP TABLE [IF EXISTS] X ...
          queryTable = findTableAfter(query, queryMethod + " IF EXISTS");
          if(queryTable.equals("N/A")) {queryTable = findTableAfter(query, queryMethod);}
          break;
        case CREATE_INDEX:        // CREATE INDEX [IF NOT EXISTS] X ...
        case CREATE_KEYSPACE:       // CREATE KEYSPACE [IF NOT EXISTS] X ...
        case CREATE_MATERIALIZED_VIEW:  // CREATE MATERIALIZED VIEW [IF NOT EXISTS] X ...
        case CREATE_TABLE:        // CREATE TABLE [IF NOT EXISTS] X ...
          queryTable = findTableAfter(query, queryMethod + " IF NOT EXISTS");
          if(queryTable.equals("N/A")) {queryTable = findTableAfter(query, queryMethod);}
          break;
        case TRUNCATE:          // TRUNCATE [TABLE] X
          queryTable = findTableAfter(query, queryMethod + " TABLE");
          if(queryTable.equals("N/A")) {queryTable = findTableAfter(query, queryMethod);}
          break;
      }
      return String.format("Cassandra.%s - %s", definitionMethod.toString(), queryTable);
    }
    return "Cassandra";
  }

  private ManipulationMethod getManipulationMethod(String query) {
    ManipulationMethod retMethod = ManipulationMethod.NOT_FOUND;
    String upperQuery = query.toUpperCase();
    for(String method: CASSANDRA_MANIPULATION_METHODS) {
      if(upperQuery.contains(method)) {
        retMethod = ManipulationMethod.valueOf(method.replace(' ', '_'));
        break;
      }
    }
    return retMethod;
  }

  private DefinitionMethod getDefininitionMethod(String query) {
    DefinitionMethod retMethod = DefinitionMethod.NOT_FOUND;
    String upperQuery = query.toUpperCase();
    for(String method: CASSANDRA_DEFINITION_METHODS) {
      if(upperQuery.contains(method)) {
        retMethod = DefinitionMethod.valueOf(method.replace(' ', '_'));
        break;
      }
    }
    return retMethod;
  }

  private String findTableAfter(String query, String after) {
    // Regex to find the first word (containing only alphanumeric, period, or underscore characters)
    // that occurs after the String after
    Pattern findTablePattern = Pattern.compile(Pattern.quote(after) + " ([\\w.]+)");
    Matcher matcher = findTablePattern.matcher(query);
    if(matcher.find()) {
      return matcher.group(1);
    } else {
      return "N/A";
    }
  }

  public static Builder newBuilder() { return new Builder();}
}
