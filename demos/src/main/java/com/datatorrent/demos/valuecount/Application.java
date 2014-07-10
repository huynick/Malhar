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
package com.datatorrent.demos.valuecount;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.python.google.common.base.Splitter;
import org.python.google.common.collect.Iterables;
import org.python.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.algo.UniqueCounterValue;
import com.datatorrent.lib.algo.UniqueValueCount;
import com.datatorrent.lib.db.jdbc.JDBCLookupCacheBackedOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.stream.StreamDuplicater;
import com.datatorrent.lib.util.KeyValPair;
import com.google.common.base.Strings;


/**
 * This application demonstrates the UniqueValueCount operator. It uses an
 * input operator which generates random key value pairs and simultaneously
 * emits them to the UniqueValueCount operator and keeps track of the number 
 * of unique values per key to emit to the verifier.
 */
@ApplicationAnnotation(name = "ValCount")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomKeyValGenerator randGen = dag.addOperator("RandomGenerator", new RandomKeyValGenerator());
    UniqueValueCount<Integer> valCount = dag.addOperator("ValueCounter", new UniqueValueCount<Integer>());
    ConsoleOutputOperator consOut = dag.addOperator("ConsoleOutput", new ConsoleOutputOperator());
    dag.getMeta(valCount).getAttributes().put(Context.OperatorContext.INITIAL_PARTITION_COUNT, 4);
    dag.getMeta(valCount).getMeta(valCount.output).getAttributes().put(Context.PortContext.UNIFIER_LIMIT, 2);
    StreamDuplicater<KeyValPair<Integer, Integer>> dup = dag.addOperator("Duplicator", new StreamDuplicater<KeyValPair<Integer, Integer>>());
    CountVerifier verifier = dag.addOperator("Verifier", new CountVerifier());
    ConsoleOutputOperator successOutput = dag.addOperator("SuccessOutput", new ConsoleOutputOperator());
    successOutput.setStringFormat("Success %d");
    ConsoleOutputOperator failureOutput = dag.addOperator("FailureOutput", new ConsoleOutputOperator());
    failureOutput.setStringFormat("Failure %d");
    
    UniqueCounterValue<Integer> successcounter = dag.addOperator("SuccessCounter", new UniqueCounterValue<Integer>());
    UniqueCounterValue<Integer> failurecounter = dag.addOperator("FailureCounter", new UniqueCounterValue<Integer>());

    dag.addStream("DataIn", randGen.outport, valCount.input);
    dag.addStream("Duplicate", valCount.output, dup.data);
    dag.addStream("Validate", dup.out1, verifier.recIn);
    dag.addStream("TrueCountIn", randGen.verport, verifier.trueIn);
    dag.addStream("SuccessCount", verifier.successPort, successcounter.data);
    dag.addStream("FailureCount", verifier.failurePort, failurecounter.data);
    dag.addStream("SuccessConsoleOut", successcounter.count, successOutput.input);
    dag.addStream("FailureConsoleOut", failurecounter.count, failureOutput.input);
    dag.addStream("ResultsOut", dup.out2, consOut.input);
  }
  
  public static class DbLookup extends JDBCLookupCacheBackedOperator<KeyValPair<Integer, Integer>> {

    protected static final String TABLE_NAME = "Unique_Lookup_Cache";
    
    @Override
    public void endWindow()
    {
      //Do nothing
    }
    
    @Override
    public Map<Object, Object> fetchStartupData()
    {
      return null;
    }

    @Override
    public Object getValueFor(Object key)
    {
      String query = "select col2 from " + TABLE_NAME + " where col1 = " + key;
      Statement stmt;
      try {
        stmt = store.getConnection().createStatement();
        ResultSet resultSet = stmt.executeQuery(query);
        resultSet.next();
        String value = resultSet.getString(1);
        stmt.close();
        resultSet.close();
        if(!Strings.isNullOrEmpty(value)){
          List<Integer> keyValues = Lists.newArrayList();
          String[]  intValues = Iterables.toArray(Splitter.on(":").omitEmptyStrings().trimResults().split(value), String.class);
          for(String intValue : intValues){
            keyValues.add(Integer.parseInt(intValue));
          }
          return keyValues;
        }
      }
      catch (SQLException e) {
        throw new RuntimeException("while fetching key", e);
      }
      return null;
    }

    @Override
    public Map<Object, Object> bulkGet(Set<Object> keys)
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected Object getKeyFromTuple(KeyValPair<Integer, Integer> tuple)
    {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}
