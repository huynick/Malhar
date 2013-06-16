/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.util.BaseUniqueKeyValueCounter;
import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * Count unique occurrences of key,val pairs within a window, and emits one HashMap tuple per unique key,val pair. <p>
 * <br>
 * This operator is same as the combination of {@link com.datatorrent.lib.algo.UniqueKeyValCounter} followed by {@link com.datatorrent.lib.stream.HashMapToKeyValPair}<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>count</b>: emits HashMap&lt;HashMap&lt;K,V&gt;(1),Integer&gt;(1)<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for UniqueKeyValCounter&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; processes 8 Million K,V pairs/s</b></td><td>Emits one tuple per K,V pair per window</td><td>In-bound throughput
 * and number of unique K,V pairs are the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys,
 * the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String)</b>: The order of the K,V pairs in the tuple is undeterminable
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for UniqueKeyValCounter&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(K)</th><th><i>count</i>(HashMap&lt;HashMap&lt;K,V&gt;(1),Integer&gt;(1))</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=2,c=3}</td></tr>
 * <tr><td>Data (process())</td><td>{b=5}</td><td></td>/tr>
 * <tr><td>Data (process())</td><td>{c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{5ah=10}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=4,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1,c=3}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{5ah=10}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=4,b=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=3,b=2}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{{a=1}=2}<br>{{5ah=10}=2}<br>{{b=5}=2}<br>{{b=2}=3}<br>{{c=1000}=1}<br>{{c=3}=3}<br>{{a=4}=2}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre <amol@malhar-inc.com>
 */
public class UniqueKeyValCounterEach<K,V> extends BaseUniqueKeyValueCounter<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * Reference counts tuples
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      for (Map.Entry<K,V> e: tuple.entrySet()) {
        processTuple(e.getKey(), e.getValue());
      }
    }
  };
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<HashMap<HashMap<K,V>, Integer>> count = new DefaultOutputPort<HashMap<HashMap<K,V>, Integer>>(this);

  /**
   * Emits one HashMap as tuple
   */
  @Override
  public void endWindow()
  {
    HashMap<HashMap<K,V>, Integer> tuple;
    for (Map.Entry<HashMap<K,V>, MutableInt> e: map.entrySet()) {
      tuple = new HashMap<HashMap<K,V>,Integer>(1);
      tuple.put(e.getKey(), e.getValue().toInteger());
      count.emit(tuple);
    }
    map.clear();
  }
}