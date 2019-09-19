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

import com.datastax.driver.core.GuavaCompatibility;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;

public class GuavaCompatibilityUtil {
  private static final GuavaCompatibilityUtil INSTANCE = new GuavaCompatibilityUtil();
  private final boolean isGuavaCompatibilityFound;

  private GuavaCompatibilityUtil() {
    boolean check;
    try {
      Class.forName("com.datastax.driver.core.GuavaCompatibility");
      GuavaCompatibility.class.getMethod("transform", ListenableFuture.class, Function.class);
      check = true;
    } catch (ClassNotFoundException | NoSuchMethodException ignore) {
      check = false;
    }
    this.isGuavaCompatibilityFound = check;
  }

  static boolean isGuavaCompatibilityFound() {
    return INSTANCE.isGuavaCompatibilityFound;
  }

}
