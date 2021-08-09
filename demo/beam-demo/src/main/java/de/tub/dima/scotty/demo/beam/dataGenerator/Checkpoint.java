package de.tub.dima.scotty.demo.beam.dataGenerator;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * The checkpoint is simply the last value produced.
 */
@DefaultCoder(AvroCoder.class)
public class Checkpoint implements UnboundedSource.CheckpointMark {
    private final int lastEmittedKey;
    private final int lastEmittedValue;

    private final Instant startTime;

    @SuppressWarnings("unused") // For AvroCoder
    private Checkpoint() {
        this.lastEmittedKey = 1;
        this.lastEmittedValue = 1;
        this.startTime = Instant.now();
    }


    /** Creates a checkpoint mark reflecting the last emitted value. */
    public Checkpoint(int lastEmittedKey, int lastEmittedValue, Instant startTime) {
        this.lastEmittedKey = lastEmittedKey;
        this.lastEmittedValue = lastEmittedValue;
        this.startTime = startTime;
    }

    /** Returns the last value emitted by the reader. */
    public Integer getLastEmittedKey() {
        return lastEmittedKey;
    }

    public Integer getLastEmittedValue() {
        return lastEmittedValue;
    }

    /** Returns the time the reader was started. */
    public Instant getStartTime() {
        return startTime;
    }


    @Override
    public void finalizeCheckpoint() throws IOException {}
}