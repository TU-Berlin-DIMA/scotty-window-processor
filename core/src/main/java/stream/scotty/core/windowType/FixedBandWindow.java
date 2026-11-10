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

import stream.scotty.core.WindowCollector;

public class FixedBandWindow implements ContextFreeWindow {
    /**
     * This window starts at the defined start timestamp and ends at start + size.
     * Scotty will continue to add tuples to one big slice, after the result of the window aggregation has been output.
     * Therefore, the execution of this window type has to be stopped manually.
     */
    private final WindowMeasure measure;
    private final long start;
    private final long size;

    /**
     * @param measure WindowMeasurement, has to be time
     * @param start start timestamp of the fixed-band window
     * @param size size of the fixed-band window
     */
    public FixedBandWindow(WindowMeasure measure, long start, long size) {
        this.measure = measure;
        this.size = size;
        this.start = start;
    }

    public long getSize() {
        return size;
    }

    public long getStart() {
        return start;
    }

    public WindowMeasure getWindowMeasure() { return measure; }

    @Override
    public long assignNextWindowStart(long position) {
        if(position == Long.MAX_VALUE || position < getStart()){
            //at the first invocation is position == Long.MAX_VALUE, returns the start timestamp of the window
            return getStart();
        }else if(position >= getStart() && position < getStart()+size){
            //returns end timestamp of the window
            return getStart()+size;
        }else{
            //all tuples arriving after the window end are collected in one big slice -> manual termination required
            return Long.MAX_VALUE;
        }
    }

    @Override
    public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
        //triggers the window, if it started after lastWatermark and ended before currentWatermark
        long windowStart = getStart();
        if(lastWatermark <= windowStart+size && windowStart+size <= currentWatermark) {
            aggregateWindows.trigger(windowStart, windowStart + size, measure);
        }
    }

    @Override
    public long clearDelay() {
        return size;
    }

    @Override
    public String toString() {
        return "Fixed Band Window{" +
                "measure=" + measure +
                ", start="+ start +
                ", size=" + size +
                '}';
    }

}
