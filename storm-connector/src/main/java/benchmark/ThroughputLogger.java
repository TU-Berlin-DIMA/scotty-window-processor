package benchmark;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputLogger extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger.class);

    private long totalReceived = 0;
    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private int elementSize;
    private long logfreq;

    public ThroughputLogger(int elementSize, long logfreq) {
        this.elementSize = elementSize;
        this.logfreq = logfreq;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        totalReceived++;
        if (totalReceived % logfreq == 0) {
            // throughput over entire time
            long now = System.currentTimeMillis();

            // throughput for the last "logfreq" elements
            if (lastLogTimeMs == -1) {
                // init (the first)
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            } else {
                long timeDiff = now - lastLogTimeMs;
                long elementDiff = totalReceived - lastTotalReceived;
                double ex = (1000 / (double) timeDiff);
                //LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. ",timeDiff, elementDiff, elementDiff * ex);

                ThroughputStatistics.getInstance().addThrouputResult(elementDiff * ex);
                //System.out.println(ThroughputStatistics.getInstance().toString());
                // reinit
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            }
        }
        //collector.emit(new Values("tuple", input.getValue(0), input.getValue(1), input.getValue(3)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("type", "key", "value", "ts"));
    }
}
