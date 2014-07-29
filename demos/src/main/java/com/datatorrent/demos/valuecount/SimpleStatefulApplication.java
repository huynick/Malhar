package com.datatorrent.demos.valuecount;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.algo.UniqueValueCount;
import com.datatorrent.lib.io.ConsoleOutputOperator;

public class SimpleStatefulApplication implements StreamingApplication
{
  @SuppressWarnings("unchecked")
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomKeyValGenerator randGen = dag.addOperator("RandomGenerator", new RandomKeyValGenerator());
    UniqueValueCount<Integer> valCount = dag.addOperator("ValueCounter", new UniqueValueCount<Integer>());
    ConsoleOutputOperator consOut = dag.addOperator("ConsoleOutput", new ConsoleOutputOperator());
    StatefulUniqueCount uniqueUnifier = dag.addOperator("Unique", new StatefulUniqueCount());
    
    @SuppressWarnings("rawtypes")
    DefaultOutputPort valOut = valCount.output;
    @SuppressWarnings("rawtypes")
    DefaultOutputPort uniqueOut = uniqueUnifier.output;
    
    dag.addStream("DataIn", randGen.outport, valCount.input);
    dag.addStream("UnifyWindows", valOut, uniqueUnifier.input);
    dag.addStream("ResultsOut", uniqueOut, consOut.input);
  }
}
