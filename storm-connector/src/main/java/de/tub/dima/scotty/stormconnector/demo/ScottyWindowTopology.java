package de.tub.dima.scotty.stormconnector.demo;


import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.stormconnector.ScottyBolt;
import de.tub.dima.scotty.stormconnector.demo.windowFunctions.sumWindowFunction;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
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
        conf.setNumWorkers(1);
        conf.setMaxTaskParallelism(1);
        //Disable Acking
        conf.setNumAckers(0);

        int numRandomKeys = 1;
        int lag = 1;//ms
        String windowType = "sliding";
        String topology = " ";

        if (topology.equals("scotty")) {
            ScottyBolt scottyBolt = new ScottyBolt<Integer, Integer>(new sumWindowFunction());
            switch (windowType) {
                case "tumbling":
                    scottyBolt.addWindow(new TumblingWindow(WindowMeasure.Time, 1000));
                    builder.setSpout("integer", new RandomIntegerSpout(numRandomKeys, lag, false));
                    break;
                case "sliding":
                    scottyBolt.addWindow(new SlidingWindow(WindowMeasure.Time, 1000, 250));
                    builder.setSpout("integer", new RandomIntegerSpout(numRandomKeys, lag, false));
                    break;
                case "session":
                    scottyBolt.addWindow(new SessionWindow(WindowMeasure.Time, 1000));
                    builder.setSpout("integer", new RandomIntegerSpout(numRandomKeys, lag, true));
                    break;
            }
            builder.setBolt("scottyWindow", scottyBolt, numRandomKeys).fieldsGrouping("integer", new Fields("key"));
            builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("scottyWindow");

        } else {
            builder.setSpout("integer", new RandomIntegerSpout(numRandomKeys, lag, false));
            switch (windowType) {
                case "tumbling":
                    builder.setBolt("tumblingsum", new TumblingWindowSumBolt()
                            .withTimestampField("ts")
                            .withWatermarkInterval(Duration.of(1000))//1 Sec Watermark
                            .withTumblingWindow(Duration.of(1000)), numRandomKeys)
                            .fieldsGrouping("integer", new Fields("key"));
                    builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("tumblingsum");
                    break;
                case "sliding":
                    builder.setBolt("slidingsum", new SlidingWindowSumBolt()
                            .withTimestampField("ts")
                            .withWatermarkInterval(Duration.of(1000))//1 Sec Watermark
                            .withWindow(Duration.of(5000), Duration.of(1000)), numRandomKeys)
                            .fieldsGrouping("integer", new Fields("key"));
                    builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("slidingsum");
                    break;
            }
        }

        cluster.submitTopology("testTopology", conf, builder.createTopology());
        //cluster.killTopology("testTopology");
        // cluster.shutdown();

    }
}