package benchmark;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;


public class LoadGeneratorSource extends BaseRichSpout {

    private static int maxBackpressure = 5000;
    private final long runtime;

    private static final Logger LOG = LoggerFactory.getLogger(LoadGeneratorSource.class);

    private final int throughput;
    private boolean running;

    private final List<Pair<Long, Long>> gaps;
    private int currentGapIndex;

    private long nextGapStart = 0;
    private long nextGapEnd;

    private long timeOffset;
    private Random random;
    private int counter = 0;
    private long eventTime = 0;
    private long msgId = 0;


    private SpoutOutputCollector collector;


    public LoadGeneratorSource(long runtime, int throughput, final List<Pair<Long, Long>> gaps) {

        this.throughput = throughput;
        this.gaps = gaps;
        this.random = new Random();
        this.runtime = runtime;
        this.running = true;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        ThroughputStatistics.getInstance().pause(false);

        long endTime = System.currentTimeMillis() + runtime;
        while (running) {
            long startTs = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                try {
                    emitValue(readNextTuple());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            while (System.currentTimeMillis() < startTs + 1000) {
                // active waiting
            }

            if (endTime <= System.currentTimeMillis())
                setRunning(false);
        }
    }

    private void emitValue(final Quartet<String, Integer, Long, Long> tuple) {

        if (tuple.getValue3() > nextGapStart) {
            ThroughputStatistics.getInstance().pause(true);
            //System.out.println("in Gap");
            if (tuple.getValue3() > this.nextGapEnd) {
                ThroughputStatistics.getInstance().pause(false);
                this.currentGapIndex++;
                if (currentGapIndex < gaps.size()) {
                    this.nextGapStart = this.gaps.get(currentGapIndex).getValue0() + this.timeOffset;
                    this.nextGapEnd = this.nextGapStart + this.gaps.get(currentGapIndex).getValue1();
                }
            } else
                return;
        }
        //GetValue 3 for Processing Time, Get Value 2 for Event-time
        collector.emit(new Values("tuple", tuple.getValue0(), tuple.getValue1(), tuple.getValue3()), ++msgId);
    }

    private Quartet<String, Integer, Long, Long> readNextTuple() throws Exception {
        return new Quartet<>("key", random.nextInt(), ++eventTime, System.currentTimeMillis());
        //return new Quartet<>("key", ++counter, ++eventTime, System.currentTimeMillis());
    }

    @Override
    public void close() {
        this.running = false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "key", "value", "ts"));
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

}
