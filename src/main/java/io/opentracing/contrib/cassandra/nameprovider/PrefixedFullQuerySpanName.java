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
package io.opentracing.contrib.cassandra.nameprovider;

/**
 * @author Jordan J Lopez
 * Returns a custom prefix and the full query as the span name
 */
public class PrefixedFullQuerySpanName implements QuerySpanNameProvider {

  private String prefix;

  public static class Builder implements QuerySpanNameProvider.Builder {

    @Override
    public QuerySpanNameProvider build() {
      return new PrefixedFullQuerySpanName("Cassandra");
    }

    public QuerySpanNameProvider build(String prefix) {
      return new PrefixedFullQuerySpanName(prefix);
    }
  }

  PrefixedFullQuerySpanName(String prefix) {
    if (prefix == null || prefix.equals("")) {
      this.prefix = "";
    } else {
      this.prefix = prefix + ": ";
    }
  }

  @Override
  public String querySpanName(String query) {
    if (query == null || query.equals("")) {
      return this.prefix + "N/A";
    } else {
      return this.prefix + query;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
