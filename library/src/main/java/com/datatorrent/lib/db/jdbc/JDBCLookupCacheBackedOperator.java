/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.db.jdbc;

import com.datatorrent.api.Context;

import com.datatorrent.lib.db.cache.AbstractDBLookupCacheBackedOperator;

/**
 * This is {@link AbstractDBLookupCacheBackedOperator} which uses JDBC to fetch the value of a key from the database
 * when the key is not present in cache. </br>
 *
 * @param <T> type of input tuples </T>
 * @since 0.9.1
 */
public abstract class JDBCLookupCacheBackedOperator<T> extends AbstractDBLookupCacheBackedOperator<T>
{
  protected final JdbcStore store;

  public JDBCLookupCacheBackedOperator()
  {
    super();
    store = new JdbcStore();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.connect();
    super.setup(context);
  }

  @Override
  public void teardown()
  {
    store.disconnect();
    super.teardown();
  }

  public JdbcStore getStore()
  {
    return store;
  }
}
