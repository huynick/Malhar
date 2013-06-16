/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.DistinctMap;
import com.malhartech.engine.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.DistinctMap}<p>
 *
 */
public class DistinctMapTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    DistinctMap<String, Number> oper = new DistinctMap<String, Number>();

    TestSink sortSink = new TestSink();
    oper.distinct.setSink(sortSink);


    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();

    input.put("a", 2);
    oper.data.process(input);
    input.clear();
    input.put("a", 2);
    oper.data.process(input);

    input.clear();
    input.put("a", 1000);
    oper.data.process(input);

    input.clear();
    input.put("a", 5);
    oper.data.process(input);

    input.clear();
    input.put("a", 2);
    input.put("b", 33);
    oper.data.process(input);

    input.clear();
    input.put("a", 33);
    input.put("b", 34);
    oper.data.process(input);

    input.clear();
    input.put("b", 34);
    oper.data.process(input);

    input.clear();
    input.put("b", 6);
    input.put("a", 2);
    oper.data.process(input);
    input.clear();
    input.put("c", 9);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 8, sortSink.collectedTuples.size());
    int aval = 0;
    int bval = 0;
    int cval = 0;
    for (Object o: sortSink.collectedTuples) {
      for (Map.Entry<String, Integer> e: ((HashMap<String, Integer>)o).entrySet()) {
        String key = e.getKey();
        if (key.equals("a")) {
          aval += e.getValue().intValue();
        }
        else if (key.equals("b")) {
          bval += e.getValue().intValue();
        }
        else if (key.equals("c")) {
          cval += e.getValue().intValue();
        }
      }
    }
    Assert.assertEquals("Total for key \"a\" ", 1040, aval);
    Assert.assertEquals("Total for key \"a\" ", 73, bval);
    Assert.assertEquals("Total for key \"a\" ", 9, cval);
  }
}