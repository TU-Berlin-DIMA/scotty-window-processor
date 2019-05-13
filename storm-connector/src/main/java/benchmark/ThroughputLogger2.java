package benchmark;


import de.tub.dima.scotty.core.TimeMeasure;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This version of the Logger does not wait for a given number of tuples to arrive.
 * It reports the number of tuples that is collected since the last given amount of time has passed.
 *
 * */

public class ThroughputLogger2 extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger2.class);

    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private int batchCounter = 0;
    private long gap = TimeMeasure.seconds(1).toMilliseconds();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        lastTotalReceived++;
        long now = System.currentTimeMillis();
            // init (the first)
        if (lastLogTimeMs == -1)
            lastLogTimeMs = now;

        //If the gap amount of time has passed.
        if (now >= lastLogTimeMs + gap -100){
            double ex = (1000 / (double) gap);
            LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. Batch {}. ", gap, lastTotalReceived, lastTotalReceived* ex, ++batchCounter);
            ThroughputStatistics.getInstance().addThrouputResult(lastTotalReceived * ex);
            System.out.println(ThroughputStatistics.getInstance().toString());
            //Reinit
            lastLogTimeMs = now;
            lastTotalReceived = 0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
