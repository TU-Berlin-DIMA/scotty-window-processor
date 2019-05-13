package de.tub.dima.scotty.flinkBenchmark;

import de.tub.dima.scotty.core.TimeMeasure;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputLogger2<T> implements FlatMapFunction<T, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger2.class);

    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private int batchCounter = 0;
    private long gap = TimeMeasure.seconds(1).toMilliseconds();


    @Override
    public void flatMap(T element, Collector<Integer> collector) throws Exception {
        lastTotalReceived++;
        long now = System.currentTimeMillis();
        // init (the first)
        if (lastLogTimeMs == -1)
            lastLogTimeMs = now;

        //If the gap amount of time has passed.
        if (now >= lastLogTimeMs + gap -100){
            double ex = (1000 / (double) gap);
            LOG.error("During the last {} ms, we received {} elements. That's {} elements/second/core. Batch {}. ", gap, lastTotalReceived, lastTotalReceived* ex, ++batchCounter);
            ThroughputStatistics.getInstance().addThrouputResult(lastTotalReceived * ex);
            //System.out.println(ThroughputStatistics.getInstance().toString());
            //Reinit
            lastLogTimeMs = now;
            lastTotalReceived = 0;
        }
    }
}
