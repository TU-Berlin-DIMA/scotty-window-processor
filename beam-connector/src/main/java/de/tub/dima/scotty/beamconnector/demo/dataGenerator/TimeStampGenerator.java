package de.tub.dima.scotty.beamconnector.demo.dataGenerator;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.concurrent.ThreadLocalRandom;

public class TimeStampGenerator implements SerializableFunction<Integer, Instant> {
    private Instant eventTime;
    private double outOfOrderProbability;
    private long sessionPeriod;
    private long minLateness;
    private long maxLateness;
    private long minGap;
    private long maxGap;
    private long lastSecond;
    private Instant lastGap;

    public TimeStampGenerator() {
        this.eventTime = new Instant(0);
        this.lastGap = new Instant(0);
        this.outOfOrderProbability = 0;
        this.minLateness = 0;
        this.maxLateness = 0;
        this.sessionPeriod = 0;
        this.minGap = 0;
        this.maxGap = 0;
    }

    @Override
    public Instant apply(Integer input) {
        //Increase the event-time at every 1 ms in processing-time
        long currentTime = System.currentTimeMillis();
        if(currentTime> lastSecond){
            eventTime = eventTime.plus(Duration.millis(1));
            lastSecond = currentTime;
        }

        // With given probability data is out-of-order
        // We make sure the tuple does not have negative timestamp
        if (ThreadLocalRandom.current().nextDouble() <= (outOfOrderProbability / 100)) {
            //We make sure the tuple does not have negative or 0 timestamp
            long bigger = Math.max(eventTime.minus(Duration.millis(ThreadLocalRandom.current().nextLong(minLateness, maxLateness+1))).getMillis(), 1);
            return new Instant(bigger);
        }

        //A session gap is created every sessionPeriod
        //Event time is shifted to future to simulate session gap
        if (sessionPeriod!=0 && eventTime.isAfter(lastGap.plus(Duration.millis(sessionPeriod)))) {
            long sessionGap = ThreadLocalRandom.current().nextLong(minGap, maxGap + 1);
            try {
                System.out.printf("\nWaiting for session gap");
                Thread.sleep(sessionGap);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            eventTime = eventTime.plus(Duration.millis(sessionGap));
            lastGap = eventTime;
        }

        return eventTime;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TimeStampGenerator)) {
            return false;
        }
        TimeStampGenerator that = (TimeStampGenerator) other;
        return that.lastGap.equals(that.lastGap);
    }
}