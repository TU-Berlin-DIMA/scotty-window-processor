package de.tub.dima.scotty.stormconnector.demo.windowFunctions.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

public class CountBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        collector.emit(new Values(inputWindow.get().size()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}