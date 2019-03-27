package benchmark.jobs;

import benchmark.LoadGeneratorSource;
import benchmark.ThroughputLogger;
import benchmark.ThroughputStatistics;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.stormconnector.SlidingWindowSumBolt;
import de.tub.dima.scotty.stormconnector.TumblingWindowSumBolt;
import de.tub.dima.scotty.stormconnector.demo.PrinterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.javatuples.Pair;

import static de.tub.dima.scotty.core.TimeMeasure.seconds;
import static java.lang.Math.toIntExact;

import java.util.List;
import java.util.Random;

public class StormBenchmarkJob {
    public StormBenchmarkJob(List<Window> assigners,long runtime, int throughput, List<Pair<Long, Long>> gaps) {
        LocalCluster cluster = new LocalCluster();
        TopologyBuilder builder = new TopologyBuilder();

        int parallelism_hint = 1;
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);
        //conf.setMaxSpoutPending(throughput);
        //Disable Acking
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);

        LoadGeneratorSource loadGenerator = new LoadGeneratorSource(runtime, throughput, gaps);
        builder.setSpout("loadGenerator", loadGenerator, parallelism_hint);

        builder.setBolt("throughputLogger", new ThroughputLogger(200, throughput), parallelism_hint).shuffleGrouping("loadGenerator");


        for (Window w : assigners) {
            if (w instanceof TumblingWindow) {
                int size = toIntExact(((TumblingWindow) w).getSize());
                conf.setMessageTimeoutSecs(size + size);
                builder.setBolt("tumblingsum", new TumblingWindowSumBolt()
                        .withTimestampField("ts")
                        .withWatermarkInterval(BaseWindowedBolt.Duration.of(1000))//1 Sec Watermark
                        .withTumblingWindow(BaseWindowedBolt.Duration.of(size)), parallelism_hint)
                        .fieldsGrouping("loadGenerator", new Fields("key"));
                //builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("tumblingsum");
            }
            if (w instanceof SlidingWindow) {
                int size = toIntExact(((SlidingWindow) w).getSize());
                int slide = toIntExact(((SlidingWindow) w).getSlide());
                conf.setMessageTimeoutSecs(size + slide + size);

                builder.setBolt("slidingsum", new SlidingWindowSumBolt()
                        .withTimestampField("ts")
                        .withWatermarkInterval(BaseWindowedBolt.Duration.of(1000))//1 Sec Watermark
                        .withWindow(BaseWindowedBolt.Duration.of(size), BaseWindowedBolt.Duration.of(slide)), parallelism_hint)
                        .fieldsGrouping("loadGenerator", new Fields("key"));
                //builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("slidingsum");
            }
        }

        //builder.setBolt("dummy", new DummyBolt(), parallelism_hint).shuffleGrouping("loadGenerator");
        cluster.submitTopology("StormBenchmarkTopology", conf, builder.createTopology());
/*        while (loadGenerator.isRunning()){

        }*/
        long endTime = System.currentTimeMillis() + runtime * 2;
        while (System.currentTimeMillis() < endTime) {
            //Wait for topology to finish
        }
        //cluster.killTopology("StormBenchmarkTopology");
    }
}

