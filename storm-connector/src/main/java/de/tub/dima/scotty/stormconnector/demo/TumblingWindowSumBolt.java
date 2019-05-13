package de.tub.dima.scotty.stormconnector.demo;

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

/*
 * Computes tumbling window average
 */
public class TumblingWindowSumBolt extends BaseWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ScottyWindowTopology.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        int sum = 0;
        Object key = null;
        List<Tuple> tuplesInWindow = inputWindow.get();
        LOG.debug("Events in current window: " + tuplesInWindow.size());
        if (tuplesInWindow.size() > 0) {
            for (Tuple tuple : tuplesInWindow) {
                if (tuple.getString(0).equals("tuple")) {
                    if (key == null) key = tuple.getValue(1);
                    sum += (int) tuple.getValue(2);
                }
            }
            collector.emit(new Values(key, sum));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "sum"));
    }
}
