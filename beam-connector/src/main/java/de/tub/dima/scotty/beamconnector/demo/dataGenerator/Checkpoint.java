/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tub.dima.scotty.beamconnector.demo.dataGenerator;

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