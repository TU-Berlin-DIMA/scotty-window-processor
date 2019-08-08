
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package de.tub.dima.scotty.stormconnector.demo.windowFunctions.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Computes sliding window sum
 */
public class SumSlidingWindowBolt extends BaseWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SumSlidingWindowBolt.class);

    private int sum = 0;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;

    }

    @Override
    public void execute(TupleWindow inputWindow) {
        /*
         * The inputWindow gives a view of
         * (a) all the events in the window
         * (b) events that expired since last activation of the window
         * (c) events that newly arrived since last activation of the window
         */
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();
        Object key = null;
        LOG.debug("Events in current window: " + tuplesInWindow.size());
        /*
         * Instead of iterating over all the tuples in the window to compute
         * the sum, the values for the new events are added and old events are
         * subtracted. Similar optimizations might be possible in other
         * windowing computations.
         */
        for (Tuple tuple : newTuples) {
            if (key == null)
                key = tuple.getValue(0);
            sum += (int) tuple.getValue(1);
        }
        for (Tuple tuple : expiredTuples) {
            if (key == null)
                key = tuple.getValue(0);
            sum -= (int) tuple.getValue(1);
        }
        collector.emit(new Values(sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }
}