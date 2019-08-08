package de.tub.dima.scotty.stormconnector.demo.windowFunctions.storm;


import de.tub.dima.scotty.stormconnector.demo.windowFunctions.QuantileTreeMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;


public class QuantileBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private final double quantile;
    private QuantileTreeMap quantileTreeMap;

    public QuantileBolt(double quantile) {
        this.quantile = quantile;
        quantileTreeMap = new QuantileTreeMap(0, quantile);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuplesInWindow = inputWindow.get();
        tuplesInWindow.forEach(x -> quantileTreeMap.addValue((int) x.getValue(1)));
        collector.emit(new Values(quantileTreeMap.getQuantile()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("avg"));
    }
}
