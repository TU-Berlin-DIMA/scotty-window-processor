package stream.scotty.flinkBenchmark;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.source.*;
import org.slf4j.*;

import java.util.*;


public class LoadGeneratorSource implements SourceFunction<Tuple4<String, Integer, Long, Long>> {

    private static int maxBackpressure = 5000;
    private final long runtime;

    private static final Logger LOG = LoggerFactory.getLogger(LoadGeneratorSource.class);

    private final int throughput;
    private boolean running = true;

    private final List<Tuple2<Long, Long>> gaps;
    private int currentGapIndex;

    private long nextGapStart = 0;
    private long nextGapEnd;

    private long timeOffset;
    private Random random;

    public LoadGeneratorSource(long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {

        this.throughput = throughput;
        this.gaps = gaps;
        this.random = new Random();
        this.runtime = runtime;
    }

    private int backpressureCounter = 0;

    @Override
    public void run(final SourceContext<Tuple4<String, Integer, Long, Long>> ctx) throws Exception {

        ThroughputStatistics.getInstance().pause(false);

        long endTime = System.currentTimeMillis() + runtime;
        while (running) {
            long startTs = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                emitValue(readNextTuple(), ctx);
            }
            while (System.currentTimeMillis() < startTs + 1000) {
                // active waiting
            }

            if(endTime <= System.currentTimeMillis())
                running = false;
        }
    }

    private void emitValue(final Tuple4<String, Integer, Long, Long> tuple3, final SourceContext<Tuple4<String, Integer, Long, Long>> ctx) {

        if (tuple3.f3 > nextGapStart) {
            ThroughputStatistics.getInstance().pause(true);
            //System.out.println("in Gap");
            if (tuple3.f3 > this.nextGapEnd) {
                ThroughputStatistics.getInstance().pause(false);
                this.currentGapIndex++;
                if (currentGapIndex < gaps.size()) {
                    this.nextGapStart = this.gaps.get(currentGapIndex).f0 + this.timeOffset;
                    this.nextGapEnd = this.nextGapStart + this.gaps.get(currentGapIndex).f1;
                }
            } else
                return;
        }
        ctx.collect(tuple3);
    }

    private Tuple4<String, Integer, Long, Long> readNextTuple() throws Exception {
        return new Tuple4<>("key", random.nextInt(), random.nextLong(), System.currentTimeMillis());

    }

    @Override
    public void cancel() {
        running = false;
    }
}
