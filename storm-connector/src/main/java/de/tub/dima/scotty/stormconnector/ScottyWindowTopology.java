package de.tub.dima.scotty.stormconnector;


import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
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
        TopologyBuilder builder = new TopologyBuilder();

        Config conf = new Config();
        conf.setDebug(false);
        String topoName = "test";
        conf.setNumWorkers(1);

        int numRandomKeys = 1;
        String windowType = "sliding";
        String topology = "scotty";

        if (topology.equals("scotty")) {
            ScottyBolt scottyBolt = new ScottyBolt<Integer, Integer>(new sumWindowFunction());
            switch (windowType) {
                case "tumbling":
                    scottyBolt.addWindow(new TumblingWindow(WindowMeasure.Time, 1000));
                    break;
                case "session":
                    scottyBolt.addWindow(new SessionWindow(WindowMeasure.Time, 1000));
                    break;
                case "sliding":
                    scottyBolt.addWindow(new SlidingWindow(WindowMeasure.Time, 1000, 2000));
                    break;
            }
            builder.setSpout("integer", new RandomIntegerSpout(numRandomKeys));
            builder.setBolt("scottyWindow", scottyBolt, numRandomKeys).fieldsGrouping("integer", new Fields("key"));
            builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("scottyWindow");

        } else {
            builder.setSpout("integer", new RandomIntegerSpout(numRandomKeys));
            switch (windowType) {
                case "tumbling":
                    builder.setBolt("tumblingsum", new TumblingWindowSumBolt().withTumblingWindow(Duration.of(1000)), numRandomKeys).fieldsGrouping("integer", new Fields("key"));
                    builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("tumblingsum");
                    break;
                case "sliding":
                    builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(Duration.seconds(5)), numRandomKeys).fieldsGrouping("integer", new Fields("key"));
                    builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("slidinggsum");
                    break;
            }
        }

        //StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        cluster.submitTopology(topoName, conf, builder.createTopology());
        //Utils.sleep(10000);
        //cluster.killTopology(topoName);
        // cluster.shutdown();

    }
}