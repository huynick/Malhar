package com.datatorrent.lib.util;
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
import java.util.ArrayList;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

/**
 * Unifier for array list output port.
 *
 * @since 0.3.3
 */
public class UnifierArrayList<K> implements Unifier<ArrayList<K>>
{
  // merged list
  private ArrayList<K> mergedList;
  
  @Override
  public void beginWindow(long arg0)
  {
    mergedList = new ArrayList<K>();
  }

  @Override
  public void endWindow()
  {
    mergedport.emit(mergedList);
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub
    
  }

  /**
   * Output port
   */
  public final transient DefaultOutputPort<ArrayList<K>> mergedport = new DefaultOutputPort<ArrayList<K>>();
  
  @Override
  public void process(ArrayList<K> tuple)
  {
    for (int i = 0; i < tuple.size(); i++) {
      mergedList.add(tuple.get(i));
    }
  }

}
