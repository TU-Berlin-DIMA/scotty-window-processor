package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.*;

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
        return recordStamp + getSlide() - (recordStamp) % getSlide();
    }

    public static long getWindowStartWithOffset(long timestamp, long windowSize) {
        return timestamp - (timestamp  + windowSize) % windowSize;
    }

    @Override
    public void triggerWindows(WindowCollector collector, long lastWatermark, long currentWatermark) {
        long lastStart  = getWindowStartWithOffset(currentWatermark, slide);

        for (long windowStart = lastStart; windowStart + size > lastWatermark; windowStart -= slide) {
            if (windowStart>=0 && windowStart + size <= currentWatermark + 1)
                collector.trigger(windowStart, windowStart + size);
        }

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
