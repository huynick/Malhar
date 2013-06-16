/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.FirstTillMatchString;
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
 * Performance tests for {@link com.datatorrent.lib.algo.FirstTillMatchString}<p>
 *
 */
public class FirstTillMatchStringBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FirstTillMatchStringBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings( {"SleepWhileInLoop", "unchecked"})
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    FirstTillMatchString<String> oper = new FirstTillMatchString<String>();
    TestSink matchSink = new TestSink();
    oper.first.setSink(matchSink);
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      matchSink.clear();
      oper.beginWindow(0);
      HashMap<String, String> input = new HashMap<String, String>();
      input.put("a", "4");
      input.put("b", "20");
      input.put("c", "1000");
      oper.data.process(input);
      input.clear();
      input.put("a", "2");
      oper.data.process(input);
      input.put("a", "3");
      input.put("b", "20");
      input.put("c", "1000");
      oper.data.process(input);
      input.clear();
      input.put("a", "4");
      input.put("b", "21");
      input.put("c", "1000");
      oper.data.process(input);
      input.clear();
      input.put("a", "6");
      input.put("b", "20");
      input.put("c", "5");
      oper.data.process(input);
      oper.endWindow();
    }

    Assert.assertEquals("number emitted tuples", 2, matchSink.collectedTuples.size());
    int atotal = 0;
    Integer aval;
    for (Object o: matchSink.collectedTuples) {
      aval = Integer.parseInt(((HashMap<String, String>)o).get("a"));
      atotal += aval.intValue();
    }
    Assert.assertEquals("Value of a was ", 6, atotal);
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 17));
  }
}