package de.tub.dima.scotty.demo.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class DataGeneratorSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorSpout.class);
    private SpoutOutputCollector collector;
    private long msgId = 0;
    private int numberOfKeys;
    private long eventTime = -1;
    private Random generator;
    private int value=0;
    private long throughputLimit;

    public DataGeneratorSpout() {
        this.numberOfKeys = 1;
        this.generator = new Random();
        this.throughputLimit = 1000;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (throughputLimit != 0) {
            long startTs = System.currentTimeMillis();
            for (int i = 0; i < throughputLimit; i++) {
                    collector.emit(new Values(generator.nextInt(numberOfKeys), ++value, ++eventTime), ++msgId);
            }
            while (System.currentTimeMillis() < startTs + 1000) {
                // active waiting
            }
        }
        else {
            while (true) {
                    collector.emit(new Values(generator.nextInt(numberOfKeys), ++value, ++eventTime), ++msgId);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value", "ts"));
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