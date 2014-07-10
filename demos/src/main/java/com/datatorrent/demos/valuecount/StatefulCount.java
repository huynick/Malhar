package com.datatorrent.demos.valuecount;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Exchanger;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.db.jdbc.JDBCLookupCacheBackedOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.google.common.collect.Maps;

public class StatefulCount
{
  private static final String INMEM_DB_URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
  private static final String INMEM_DB_DRIVER = "org.hsqldb.jdbcDriver";
  private static int windowID;
  protected static final String TABLE_NAME = "Test_Lookup_Cache";

  private final static Exchanger<Map<Object, Object>> bulkValuesExchanger = new Exchanger<Map<Object, Object>>();


  public static class TestJDBCLookupCacheBackedOperator extends JDBCLookupCacheBackedOperator<KeyValPair<Integer, Integer>>
  {
    public final transient DefaultInputPort<KeyValPair<Integer, Integer>> input = new DefaultInputPort<KeyValPair<Integer, Integer>>() {
      @Override
      public void process(KeyValPair<Integer, Integer> tuple)
      {
        String query = "select from " + TABLE_NAME + " where col1 = " + tuple.getKey() + " and col2 = " + tuple.getValue();
        Statement stmt;
        try {
          stmt = store.getConnection().createStatement();
          ResultSet resultSet = stmt.executeQuery(query);
          if (!resultSet.next()) {
            query = "insert into " + TABLE_NAME + "(" + tuple.getKey() + ", " + tuple.getValue() + ", " + windowID + ")";
          }
        } catch (SQLException e) {
          throw new RuntimeException("while processing tuples", e);
        }
      }
    };
    
    public final transient DefaultOutputPort<KeyValPair<Integer, Integer>> output = new DefaultOutputPort<KeyValPair<Integer, Integer>>();

    @Override
    public Integer getKeyFromTuple(KeyValPair<Integer, Integer> tuple)
    {
      return tuple.getKey();
    }

    @Override
    public Object getValueFor(Object key)
    {
      String query = "select col2 from " + TABLE_NAME + " where col1 = " + key;
      Statement stmt;
      try {
        int count = 0;
        stmt = store.getConnection().createStatement();
        ResultSet resultSet = stmt.executeQuery(query);
        while (resultSet.next()) {
          count++;
        }
        stmt.close();
        resultSet.close();
        return count;
      } catch (SQLException e) {
        throw new RuntimeException("while fetching key", e);
      }
    }

    @Override
    public Map<Object, Object> bulkGet(Set<Object> keys)
    {
      StringBuilder builder = new StringBuilder("(");
      for (Object k : keys) {
        builder.append(k);
        builder.append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append(")");
      String query = "select col1, col2 from " + TABLE_NAME + " where col1 in " + builder.toString();

      try {
        Statement statement = store.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        Map<Object, Object> values = Maps.newHashMap();
        while (resultSet.next()) {
          values.put(resultSet.getInt(1), resultSet.getString(2));
        }
        bulkValuesExchanger.exchange(values);
        return values;
      } catch (SQLException e) {
        throw new RuntimeException("while fetching multiple keys", e);
      } catch (InterruptedException e) {
        throw new RuntimeException("interrupted while multiple keys", e);
      }
    }

    @Override
    public Map<Object, Object> fetchStartupData()
    {
      return null;
    }
    
    @Override
    public void endWindow() {
      String query = "select col1 from " + TABLE_NAME;
      Statement stmt;
      try {
        stmt = store.getConnection().createStatement();
        ResultSet resultSet = stmt.executeQuery(query);
        while (resultSet.next()) {
          query = "select col2 from " + TABLE_NAME + " where col2 = " + resultSet.getString(1);
          ResultSet countResultSet = stmt.executeQuery(query);
          int count = 0;
          while(countResultSet.next()) {
            count++;
          }
          output.emit(new KeyValPair<Integer, Integer>(Integer.getInteger(resultSet.getString(1)), count));
        }
      } catch (SQLException e) {
        throw new RuntimeException("while emitting tuples", e);
      }
    }

  }
}
