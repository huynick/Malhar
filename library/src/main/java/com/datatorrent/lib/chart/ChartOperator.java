/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.chart;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.PartitionableOperator;

/**
 * This is the base class for all chart operators
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class ChartOperator extends BaseOperator implements PartitionableOperator
{
  /**
   * The different types of chart
   */
  public enum Type
  {
    /**
     * Line - One point for each data item. Both X-axis and Y-axis are numbers. X-axis is usually a time-series
     */
    LINE,
    /**
     * Candle - Four points for each data item (open, close, high, low). Both X-axis and Y-axis are numbers. X-axis is usually a time series
     */
    CANDLE,
    /**
     * Enumerated - X-axis is an enumeration set. Y-axis is a number
     */
    ENUM,
    /**
     * Histogram - Like ENUM, except X-axis is a set of ranges.
     */
    HISTOGRAM,
  }

  /**
   * Gets the chart type
   *
   * @return The chart type
   */
  public abstract Type getChartType();

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity)
  {
    // prevent partitioning
    List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(1);
    newPartitions.add(partitions.iterator().next());
    return newPartitions;
  }

}