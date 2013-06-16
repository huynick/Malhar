/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.OrderByKeyDesc;
import com.datatorrent.lib.testbench.CountTestSink;
import com.malhartech.engine.TestSink;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.OrderByKey}<p>
 */
public class OrderByKeyDescBenchmark
{
  private static Logger log = LoggerFactory.getLogger(OrderByKeyDescBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    OrderByKeyDesc<String, Integer> oper = new OrderByKeyDesc<String, Integer>();
    CountTestSink countSink = new CountTestSink<HashMap<Integer, Integer>>();
    CountTestSink listSink = new CountTestSink<HashMap<String, Integer>>();
    oper.ordered_count.setSink(countSink);
    oper.ordered_list.setSink(listSink);
    oper.setOrderby("a");
    oper.setup(null);

    HashMap<String, Integer> input = new HashMap<String, Integer>();
    oper.beginWindow(0);
    int numTuples = 1000000;

    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a", i);
      input.put("b", 5);
      input.put("c", 6);
      oper.data.process(input);

      input.clear();
      input.put("b", 50);
      input.put("c", 16);
      oper.data.process(input);

      input.clear();
      input.put("a", i + 2);
      input.put("b", 2);
      input.put("c", 3);
      oper.data.process(input);

      input.clear();
      input.put("a", 1);
      input.put("b", 7);
      input.put("c", 4);
      oper.data.process(input);

      input.clear();
      input.put("a", 3);
      input.put("b", 23);
      input.put("c", 33);
      oper.data.process(input);

      input.clear();
      input.put("a", i + 2);
      input.put("b", 5);
      input.put("c", 6);
      oper.data.process(input);

      input.clear();
      input.put("e", 2);
      input.put("b", 5);
      input.put("c", 6);
      oper.data.process(input);
    }
    oper.endWindow();
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 7));
  }
}