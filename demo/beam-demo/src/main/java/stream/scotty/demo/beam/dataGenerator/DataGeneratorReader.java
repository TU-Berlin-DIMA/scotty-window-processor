package stream.scotty.demo.beam.dataGenerator;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Random;

public class DataGeneratorReader extends UnboundedSource.UnboundedReader<KV<Integer, Integer>> {
    private DataGeneratorSource source;
    private KV<Integer, Integer> current;
    private Random random = new Random();
    private long lastTime;
    private long now;
    private int counter = 0;
    private int throughputLimit;
    private int key = 1;

    // Initialized on first advance()
    @Nullable
    private Instant currentTimestamp;

    // Initialized in start()
    @Nullable
    private Instant firstStarted;

    public DataGeneratorReader(int throughputLimit, DataGeneratorSource source, Checkpoint mark) {
        this.source = source;
        this.throughputLimit = throughputLimit;
        if (mark == null) {
            // Because we have not emitted an element yet, and start() calls advance, we need to
            // "un-advance" so that start() produces the correct output.
            this.current = KV.of(key, random.nextInt());
        } else {
            this.current = KV.of(mark.getLastEmittedKey(), mark.getLastEmittedValue());
            this.firstStarted = mark.getStartTime();
        }
    }

    @Override
    public boolean start() throws IOException {
        if (firstStarted == null) {
            this.firstStarted = Instant.now();
            this.lastTime = this.firstStarted.getMillis();
        }
        return advance();
    }

    @Override
    public boolean advance() throws IOException {
        //Generate with limit
        if (throughputLimit != 0) {
            now = System.currentTimeMillis();
            if (this.counter < this.throughputLimit && now < this.lastTime + 1000) {
                this.counter++;
                this.current = KV.of(key, random.nextInt());
                this. currentTimestamp = this.source.timestampFn.apply(current.getValue());
                return true;
            } else {
                if (now > this.lastTime + 1000) {
                    lastTime = now;
                    counter = 0;
                }
                return false;
            }
        } else {
            this.counter++;
            this.current = KV.of(key, random.nextInt());
            this.currentTimestamp = source.timestampFn.apply(current.getValue());
            return true;
        }
    }

    @Override
    public Instant getWatermark() {
        return source.timestampFn.apply(current.getValue());
    }

    @Override
    public Checkpoint getCheckpointMark() {
        return new Checkpoint(current.getKey(), current.getValue(), firstStarted);
    }

    @Override
    public UnboundedSource<KV<Integer, Integer>, Checkpoint> getCurrentSource() {
        return source;
    }

    @Override
    public KV<Integer, Integer> getCurrent() throws NoSuchElementException {
        return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        return currentTimestamp;
    }

    @Override
    public void close() throws IOException {

    }
}