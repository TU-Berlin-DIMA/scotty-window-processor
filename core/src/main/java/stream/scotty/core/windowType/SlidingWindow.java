/*
 * Copyright The Scotty Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stream.scotty.core.windowType;

import stream.scotty.core.*;
import stream.scotty.core.*;

public class SlidingWindow implements ContextFreeWindow {

    private final WindowMeasure measure;

    /**
     * Size of the sliding window
     */
    private final long size;

    /**
     * The window slide step
     */
    private final long slide;

    public SlidingWindow(WindowMeasure measure, long size, long slide) {
        this.measure = measure;
        this.size = size;
        this.slide = slide;
    }

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }


    @Override
    public long assignNextWindowStart(long recordStamp) {
        long nextWindowStart = recordStamp + getSlide() - (recordStamp) % getSlide();
        long nextWindowEnd = recordStamp < getSize() ? getSize() : recordStamp + this.getSlide() - (recordStamp - this.getSize()) % this.getSlide();
        return Math.min(nextWindowStart, nextWindowEnd);
    }

    public static long getWindowStartWithOffset(long timestamp, long windowSize) {
        return timestamp - (timestamp  + windowSize) % windowSize;
    }

    @Override
    public void triggerWindows(WindowCollector collector, long lastWatermark, long currentWatermark) {
        long lastStart  = getWindowStartWithOffset(currentWatermark, slide);

        for (long windowStart = lastStart; windowStart + size > lastWatermark; windowStart -= slide) {
            if (windowStart>=0 && windowStart + size <= currentWatermark + 1)
                collector.trigger(windowStart, windowStart + size, measure);
        }
    }

    @Override
    public long clearDelay() {
        return size;
    }

    @Override
    public String toString() {
        return "SlidingWindow{" +
                "measure=" + measure +
                ", size=" + size +
                ", slide=" + slide +
                '}';
    }
}
