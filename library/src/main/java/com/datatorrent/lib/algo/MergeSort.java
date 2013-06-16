/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.util.AbstractBaseSortOperator;
import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * <b>performance can be further improved</b>
 * Incoming sorted list is merged into already existing sorted list. The input list is expected to be sorted. At the end of the window the resultant sorted
 * list is emitted on the output ports<b>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects ArrayList&lt;K&gt;<br>
 * <b>sort</b>: emits ArrayList&lt;K&gt;<br>
 * <b>sorthash</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>:<br>
 * <b>Specific run time checks</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for MergeSort&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 2.5 Million tuples/s on average</b></td><td>All tuples inserted one at a time</td>
 * <td>In-bound throughput (i.e. total number of tuples in the window) is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for MergeSort&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>datalsit</i>(ArrayList&lt;K&gt;)</th><th><i>sort</i>(ArrayList&lt;K&gt;)</th><th><i>sorthash</i>(HashMap&lt;K,Integer&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td></td><td>[-4,2,20]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[-10,-5,-4]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[1,2,3,3,10,15,100]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[1,1,2]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[15,20]</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>[-10,-10,-5,-4,-4,1,1,1,2,2,2,3,3,15,15,20,20,100]</td>
 * <td>{-10=2,-5=1,-4=2,1=3,2=3,3=2,15=2,20=2,100=1}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class MergeSort<K> extends AbstractBaseSortOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>(this)
  {
    @Override
    public void process(ArrayList<K> tuple)
    {
      processTuple(tuple);
    }
  };
  @OutputPortFieldAnnotation(name = "sort")
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>(this);
  @OutputPortFieldAnnotation(name = "sorthash", optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> sorthash = new DefaultOutputPort<HashMap<K, Integer>>(this);

  /*
   * <b>Currently implemented with individual keys inserted. Needs to be reimplemented</b>
   *
   */
  @Override
  public void processTuple(ArrayList<K> tuple)
  {
    super.processTuple(tuple);
  }


  @Override
  public boolean doEmitList()
  {
    return sort.isConnected();
  }

  @Override
  public boolean doEmitHash()
  {
    return sorthash.isConnected();
  }

  @Override
  public void emitToList(ArrayList<K> list)
  {
    sort.emit(list);
  }

  @Override
  public void emitToHash(HashMap<K,Integer> map)
  {
    sorthash.emit(map);
  }
}