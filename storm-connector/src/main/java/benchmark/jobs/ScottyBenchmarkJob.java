package benchmark.jobs;

import benchmark.LoadGeneratorSource;
import benchmark.ThroughputLogger;
import benchmark.WatermarkGenerator;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.stormconnector.demo.DummyBolt;
import de.tub.dima.scotty.stormconnector.demo.PrinterBolt;
import de.tub.dima.scotty.stormconnector.demo.ScottyBolt;
import de.tub.dima.scotty.stormconnector.demo.windowFunctions.sumWindowFunction;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.javatuples.Pair;

import java.util.List;

import static java.lang.Math.toIntExact;

public class ScottyBenchmarkJob {
    public ScottyBenchmarkJob(List<Window> assigners, long runtime, int throughput, List<Pair<Long, Long>> gaps) throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        TopologyBuilder builder = new TopologyBuilder();

        int parallelism_hint = 1;
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);
        conf.setMaxSpoutPending(throughput);
        //Disable Acking
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);

//        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 100000);
        LoadGeneratorSource generator = new LoadGeneratorSource(runtime, throughput, gaps);
        builder.setSpout("loadGenerator", generator,parallelism_hint);
        builder.setBolt("throughputLogger", new ThroughputLogger(200, throughput), parallelism_hint).shuffleGrouping("loadGenerator");

        builder.setBolt("watermarkGenerator", new WatermarkGenerator(), parallelism_hint).shuffleGrouping("loadGenerator");
        ScottyBolt scottyBolt = new ScottyBolt<Integer, Integer>(new sumWindowFunction());
        for (Window w : assigners) {
            int size = toIntExact(((SlidingWindow) w).getSize());
            conf.setMessageTimeoutSecs(size * 2);
            scottyBolt.addWindow(w);
        }
        builder.setBolt("scottySlidingWindow", scottyBolt, parallelism_hint).fieldsGrouping("watermarkGenerator", new Fields("key"));
        //builder.setBolt("printer", new PrinterBolt(), parallelism_hint).shuffleGrouping("scottySlidingWindow");

        //builder.setBolt("dummy", new DummyBolt(), parallelism_hint).shuffleGrouping("loadGenerator");
        cluster.submitTopology("StormBenchmarkTopology", conf, builder.createTopology());
        long endTime = System.currentTimeMillis() + runtime * 2;

        while (System.currentTimeMillis()<endTime) {
            //Wait for topology to finish
        }
        //cluster.killTopology("StormBenchmarkTopology");
    }
}
