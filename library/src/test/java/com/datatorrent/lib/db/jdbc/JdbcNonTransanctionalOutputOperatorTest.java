/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.google.common.collect.Lists;

/**
 * Test for {@link AbstractJdbcNonTransactionableOutput Operator}
 */
public class JdbcNonTransanctionalOutputOperatorTest
{
  public static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  public static final String URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";

  private static final String TABLE_NAME = "test_event_table";
  private static String APP_ID = "JdbcOperatorTest";
  private static int OPERATOR_ID = 0;
  
  private static class TestEvent
  {
    int id;

    TestEvent(int id)
    {
      this.id = id;
    }
  }

  @BeforeClass
  public static void setup()
  {
    try {
      Class.forName(DB_DRIVER).newInstance(); 

      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (ID INTEGER)";
      stmt.executeUpdate(createTable);
    }
    catch (Throwable e) {
      DTThrowable.rethrow(e);
    }
  }

  private static void cleanTable()
  {
    try {
      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String cleanTable = "delete from " + TABLE_NAME;
      stmt.executeUpdate(cleanTable);
    } 
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestOutputOperator extends AbstractJdbcNonTransactionableOutputOperator<TestEvent>
  {
    private static final String INSERT_STMT = "INSERT INTO " + TABLE_NAME + " values (?)";

    TestOutputOperator()
    {
      cleanTable();
    }

    @Nonnull
    @Override
    protected String getUpdateCommand()
    {
      return INSERT_STMT;
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement, TestEvent tuple) throws SQLException
    {
      System.out.println(tuple.id);
      statement.setInt(1, tuple.id);
    }

    public int getNumOfEventsInStore()
    {
      Connection con;
      try {
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();

        String countQuery = "SELECT * FROM " + TABLE_NAME;
        ResultSet resultSet = stmt.executeQuery(countQuery);
        int count = 0;
        while(resultSet.next()) {
          count++;
        }
        return count;
      }
      catch (SQLException e) {
        throw new RuntimeException("fetching count", e);
      }
    }
  }

  @Test
  public void testJdbcOutputOperator()
  {
    JdbcStore store = new JdbcStore();
    store.setDbDriver(DB_DRIVER);
    store.setDbUrl(URL);
    TestOutputOperator outputOperator = new TestOutputOperator();
    
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
    outputOperator.setStore(store);
    
    outputOperator.setup(context);

    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestEvent(i));
    }

    outputOperator.beginWindow(0);
    for (TestEvent event : events) {
      outputOperator.input.process(event);
    } 
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 10, outputOperator.getNumOfEventsInStore());
  }
}

