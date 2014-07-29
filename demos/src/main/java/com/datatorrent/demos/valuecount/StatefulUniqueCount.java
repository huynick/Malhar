package com.datatorrent.demos.valuecount;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nonnull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.algo.UniqueValueCount.InternalCountOutput;
import com.datatorrent.lib.db.jdbc.JDBCLookupCacheBackedOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class StatefulUniqueCount extends JDBCLookupCacheBackedOperator<InternalCountOutput<Integer>> implements Partitioner<StatefulUniqueCount>
{
  private static final String INMEM_DB_URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
  private static final String INMEM_DB_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
  protected static final String TABLE_NAME = "Test_Lookup_Cache";
  protected Set<Integer> partitionKeys;
  protected int partitionMask;
  private long windowID;
  private transient boolean batch;
  
  public StatefulUniqueCount()
  {
    setTable(TABLE_NAME);
  }

  @Override
  protected void processTuple(InternalCountOutput<Integer> tuple)
  {

    Object key = getKeyFromTuple(tuple);
    @SuppressWarnings("unchecked")
    Set<Object> values = (Set<Object>) storeManager.get(key);
    if (values == null) {
      values = Sets.newHashSet();
    }
    values.addAll(tuple.getInternalSet());
    storeManager.put(key, values);
  }

  @Override
  public Map<Object, Object> fetchStartupData() 
  {
    return null;
  }

  @Override
  public Object processResultSet(ResultSet resultSet)
  {
    Set<Integer> valSet = new HashSet<Integer>();
    try {
      while (resultSet.next())
        valSet.add(resultSet.getInt(1));
      return valSet;
    } catch (SQLException e) {
      throw new RuntimeException("while processing the result set", e);
    }
  }

  @Override
  protected String fetchInsertQuery()
  {
    return "INSERT INTO " + TABLE_NAME + " (col1, col2, col3) VALUES (?, ?, ?)";
  }
  
  @Override
  protected String fetchGetQuery()
  {
    return "select col2 from " + TABLE_NAME + " where col1 = ?";
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      windowID = context.getId();
      setTable(TABLE_NAME);
      store.setDbDriver(INMEM_DB_DRIVER);
      store.setDbUrl(INMEM_DB_URL);
      Class.forName(INMEM_DB_DRIVER).newInstance();
      Connection con = DriverManager.getConnection(INMEM_DB_URL, new Properties());
      Statement stmt = con.createStatement();
      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (col1 INTEGER, col2 INTEGER, col3 BIGINT, CONSTRAINT pk PRIMARY KEY (col1, col2))";
      stmt.executeUpdate(createTable);
      super.setup(context);
      stmt = con.createStatement();
      ResultSet resultSet = stmt.executeQuery("SELECT col1 FROM " + TABLE_NAME + " WHERE col3 > " + windowID);
      PreparedStatement deleteStatement;
      deleteStatement = con.prepareStatement("DELETE FROM " + TABLE_NAME + " WHERE col3 > " + windowID + " AND col1 = ?");
      Set<Integer> doneKeys = new HashSet<Integer>();
      while (resultSet.next()) {
        Integer key = resultSet.getInt(1);
        if (partitionKeys.contains((key.hashCode() & partitionMask)) && !doneKeys.contains(key)) {
          doneKeys.add(key);
          deleteStatement.setInt(1, key);
          deleteStatement.executeUpdate();
        }
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(@Nonnull Object key, @Nonnull Object value)
  {
    try {
      batch = false;
      preparePutStatement(putStatement, key, value);
      if(batch) {
        putStatement.executeBatch();
        putStatement.clearBatch();
      }
    } catch (SQLException e) {
      throw new RuntimeException("while executing insert", e);
    }
  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void beginWindow(long windowID)
  {
    this.windowID = windowID;
  }

  @Override
  public void endWindow()
  {
    try {
      Statement stmt = store.getConnection().createStatement();
      String keySetQuery = "SELECT DISTINCT col1 FROM " + TABLE_NAME;
      ResultSet resultSet = stmt.executeQuery(keySetQuery);
      while (resultSet.next()) {
        int val = resultSet.getInt(1);
        @SuppressWarnings("unchecked")
        Set<Integer> valSet = (Set<Integer>) storeManager.get(val);
        output.emit(new KeyValPair<Object, Object>(val, valSet.size()));
        System.out.println("Key = " + val + " Count = " + valSet.size());
      }
    } catch (SQLException e) {
      throw new RuntimeException("While emitting tuples", e);
    }
  }

  @Override
  protected void prepareGetStatement(PreparedStatement getStatement, Object key) throws SQLException
  {
    getStatement.setInt(1, (Integer) key);
  }

  @Override
  protected void preparePutStatement(PreparedStatement putStatement, Object key, Object value) throws SQLException
  {
    @SuppressWarnings("unchecked")
    Set<Integer> valueSet = (Set<Integer>) value;
    for (Integer val : valueSet) {
      @SuppressWarnings("unchecked")
      Set<Integer> currentVals = (Set<Integer>) get(key);
      if (!currentVals.contains(val)) {
        batch = true;
        putStatement.setInt(1, (Integer) key);
        putStatement.setInt(2, (Integer) val);
        putStatement.setLong(3, windowID);
        putStatement.addBatch();
      }
    }
  }

  @Override
  protected Object getKeyFromTuple(InternalCountOutput<Integer> tuple)
  {
    return tuple.getKey();
  }

  @Override
  public Collection<com.datatorrent.api.Partitioner.Partition<StatefulUniqueCount>> definePartitions(Collection<com.datatorrent.api.Partitioner.Partition<StatefulUniqueCount>> partitions, int incrementalCapacity)
  {
    if (incrementalCapacity == 0) {
      return partitions;
    }

    final int finalCapacity = partitions.size() + incrementalCapacity;
    partitions.clear();

    Collection<Partition<StatefulUniqueCount>> newPartitions = Lists.newArrayListWithCapacity(finalCapacity);

    for (int i = 0; i < finalCapacity; i++) {
      try {
        StatefulUniqueCount statefulUniqueCount = this.getClass().newInstance();
        DefaultPartition<StatefulUniqueCount> partition = new DefaultPartition<StatefulUniqueCount>(statefulUniqueCount);
        newPartitions.add(partition);
      } catch (Throwable cause) {
        DTThrowable.rethrow(cause);
      }
    }

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), input);
    int lPartitionMask = newPartitions.iterator().next().getPartitionKeys().get(input).mask;

    // transfer the state here
    for (Partition<StatefulUniqueCount> statefulUniqueCountPartition : newPartitions) {
      StatefulUniqueCount statefulUniqueCountInstance = statefulUniqueCountPartition.getPartitionedInstance();

      statefulUniqueCountInstance.partitionKeys = statefulUniqueCountPartition.getPartitionKeys().get(input).partitions;
      statefulUniqueCountInstance.partitionMask = lPartitionMask;
    }
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<StatefulUniqueCount>> partitions)
  {}
}
