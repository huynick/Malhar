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
package com.datatorrent.lib.algo;

import java.util.ArrayList;
import java.util.HashMap;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.AbstractBaseNUniqueOperatorMap;

/**
 * Input stream of key value pairs are ordered by key, and bottom N of the ordered unique tuples per key are emitted on
 * port "top" at the end of window<p>
 * This is an end of window module<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V&gt;<br>
 * <b>bottom</b>: emits HashMap&lt;K, ArrayList&lt;HashMap&lt;V,Integer&gt;&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * N: Has to be >= 1<br>
 *
 * @since 0.3.3
 */
public class BottomNUniqueMap<K, V> extends AbstractBaseNUniqueOperatorMap<K, V>
{
  /**
   * Bottom tuples output port.
   */
  @OutputPortFieldAnnotation(name = "bottom")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<HashMap<V,Integer>>>> bottom = new DefaultOutputPort<HashMap<K, ArrayList<HashMap<V,Integer>>>>();

  /**
   * Ascending is set to false as we are looking for Bottom N
   * @return false
   */
  @Override
  public boolean isAscending()
  {
    return false;
  }

  /**
   * Emits tuple to port "bottom"
   * @param tuple
   */
  @Override
  public void emit(HashMap<K, ArrayList<HashMap<V,Integer>>> tuple)
  {
    bottom.emit(tuple);
  }
}
