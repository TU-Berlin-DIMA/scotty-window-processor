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
package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.*;

public class TumblingWindow implements ContextFreeWindow {

    private final WindowMeasure measure;
    /**
     * Size of the tumbling window
     */
    private final long size;

    public TumblingWindow(WindowMeasure measure, long size) {
        this.measure = measure;
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }

    @Override
    public long assignNextWindowStart(long recordStamp) {
        return recordStamp + getSize() - (recordStamp) % getSize();
    }

    @Override
    public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
        long lastStart = lastWatermark - ((lastWatermark + size) % size);
        for (long windowStart = lastStart; windowStart + size <= currentWatermark; windowStart += size) {
            aggregateWindows.trigger(windowStart, windowStart + size, measure);
        }
    }

    @Override
    public long clearDelay() {
        return size;
    }

    @Override
    public String toString() {
        return "TumblingWindow{" +
                "measure=" + measure +
                ", size=" + size +
                '}';
    }
}
