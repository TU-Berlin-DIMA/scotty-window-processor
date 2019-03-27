
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

package de.tub.dima.scotty.stormconnector.demo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class RandomIntegerSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomIntegerSpout.class);
    private SpoutOutputCollector collector;
    private long msgId = 0;
    private Random key;
    private Random value;
    private int incrVal = 0;
    private long lastWatermark = 0;
    private int numKeys;
    //event time should be long
    private long eventTime = 0;
    private int lag = 0;
    private int sessionStep = 1;
    private boolean sessionGap = false;

    public RandomIntegerSpout(int numKeys, int lag, boolean sessionGap) {
        this.numKeys = numKeys;
        this.sessionGap = sessionGap;
        this.lag = lag;
        this.key = new Random(42);
        this.value = new Random(43);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        collector.emit(new Values("tuple", key.nextInt(numKeys), ++incrVal, ++eventTime, ++msgId), msgId);
        //Emit Watermarks every 1 sec
        if (lastWatermark + 1000 < System.currentTimeMillis()) {
            //We want to emit watermarks with all keys since watermarks are independent of key
            for (int key = 0; key < numKeys; key++)
                collector.emit(new Values("waterMark", key, 1, eventTime, ++msgId), msgId);
            lastWatermark = System.currentTimeMillis();
        }
        Utils.sleep(lag);
        //Session Gap
        if (sessionGap) {
            if (sessionStep == 100) {
                eventTime += 1000;
                sessionStep = 0;
            }
            sessionStep++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "key", "value", "ts", "msgid"));
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL for msgId : " + msgId);
    }
}