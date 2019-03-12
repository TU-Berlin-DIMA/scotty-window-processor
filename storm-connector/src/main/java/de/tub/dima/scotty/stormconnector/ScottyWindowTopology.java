package de.tub.dima.scotty.stormconnector;


import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.stormconnector.demo.PrinterBolt;
import de.tub.dima.scotty.stormconnector.demo.RandomIntegerSpout;
import de.tub.dima.scotty.stormconnector.demo.ScottyBolt;
import de.tub.dima.scotty.stormconnector.demo.windowFunctions.sumWindowFunction;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScottyWindowTopology {

    private static final Logger LOG = LoggerFactory.getLogger(ScottyWindowTopology.class);

    public static void main(String[] args) throws Exception {
        LocalCluster cluster = new LocalCluster();
        String topology = "Scotty";
        TopologyBuilder builder = new TopologyBuilder();

        if (topology.equals("Scotty")) {
            BaseBasicBolt scottyBolt = new ScottyBolt<Integer, Integer>(new sumWindowFunction());
            ((ScottyBolt) scottyBolt).addWindow(new TumblingWindow(WindowMeasure.Time, 1000));

            builder.setSpout("integer", new RandomIntegerSpout(), 1);
            builder.setBolt("scottyWindow", scottyBolt, 1).fieldsGrouping("integer", new Fields("key"));
            builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("scottyWindow");

        } else {
            builder.setSpout("integer", new RandomIntegerSpout(), 1);
            //builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(Duration.seconds(5)), 1).fieldsGrouping("integer",new Fields("key"));
            builder.setBolt("tumblingsum", new TumblingWindowSumBolt().withTumblingWindow(Duration.of(1000)), 1)
                    .fieldsGrouping("integer", new Fields("key"));
            builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("tumblingsum");

        }
        Config conf = new Config();
        conf.setDebug(true);
        String topoName = "test";
        conf.setDebug(false);
        conf.setNumWorkers(1);
        //StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        cluster.submitTopology(topoName, conf, builder.createTopology());
        //Utils.sleep(10000);
        //cluster.killTopology(topoName);
        // cluster.shutdown();

    }
}