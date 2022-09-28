package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.WindowCollector;
import de.tub.dima.scotty.core.windowType.windowContext.WindowContext;
import org.apache.flink.api.java.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;

public class ThresholdFrame implements ForwardContextFree {

    private final WindowMeasure measure;
    private final int threshold;
    private final int attribute;
    private final long minSize;

    /**
     *  Implements Threshold Frame after the definition by Grossniklaus et al. 2016
     * @param threshold the value of the threshold
     * @param attribute the position of an attribute in a tuple which values should be compared to the threshold
     * @param minSize the minimum count of tuples in the frame, 2 tuples by default
     */
    public ThresholdFrame(int threshold, int attribute, long minSize){
        this.measure = WindowMeasure.Time;
        this.attribute = attribute;
        this.threshold = threshold;
        this.minSize = minSize;
    }

    public ThresholdFrame(int threshold){
        this(threshold, 0, 2);
    }

    public ThresholdFrame(int threshold, long minSize){
        this(threshold, 0, minSize);
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return this.measure;
    }

    @Override
    public ThresholdFrameContext createContext() {
        return new ThresholdFrameContext();
    }

    public class ThresholdFrameContext extends WindowContext<Object> {

        long count = 0;
        long gap = 0;
        long lastEnd = 0;
        private ArrayList<Long> timestamps = new ArrayList<Long>(); //holds timestamps of tuples for out-of-order processing

        @Override
        public ActiveWindow updateContext(Object o, long position) {
            int value;
            if(o instanceof Tuple){ //fetching the value of the attribute in the tuple
                value = (int)((Tuple) o).getField(attribute);
            }else{
                value = (int)o;
            }

            if (hasActiveWindows()) {
                if(value > threshold){ //begin of first frame
                    count++;
                    gap = 0;
                    timestamps.add(position);
                    addNewWindow(0, position, position);
                    return getWindow(0);
                }else{
                    return null;
                }
            }

            int fIndex = getFrame(position);

            if (position >= timestamps.get(timestamps.size() -1)) {
                //processes in-order tuples
                timestamps.add(position);

                if (fIndex == -1) {
                    if (value > threshold) {
                        addNewWindow(0, position, position);
                    }
                } else {
                    ActiveWindow f = getWindow(fIndex);
                    if (value > threshold) {
                        if (count == 0 && gap >= 1) { //open new frame
                            count++;
                            gap = 0;
                            return addNewWindow(fIndex + 1, position, position);
                        } else { //update frame
                            //append tuple to active frame
                            count++;
                            shiftEnd(f, position);
                            return f;
                        }
                    } else {
                        if (gap == 0) {
                            if (count >= minSize) { //close frame with first tuple that is below the threshold
                                shiftEnd(f, position);
                                count = 0;
                                gap++;
                                lastEnd = position;
                                return f;
                            } else { //discard frame if it is smaller than minSize
                                removeWindow(fIndex);
                                count = 0;
                                gap++;
                            }
                        } else { //tuple that is not included in any frame
                            count = 0;
                            gap++;
                        }
                    }
                }
            } else {
                //processes out-of-order tuples
                timestamps.add(position);
                Collections.sort(timestamps);

                if(value > threshold){
                    if((fIndex+1) < numberOfActiveWindows()) { //frame after this one exists
                        int index = timestamps.indexOf(position);
                        long timestampAfter = (long) timestamps.get(index + 1);
                        ActiveWindow fNext = getWindow(fIndex + 1);
                        if (timestampAfter == fNext.getStart()) { //shift frame start of next window to current tuple
                            shiftStart(fNext, position);
                            return fNext;
                        }
                        //else: simple insert, changes nothing
                    }
                    //else: current frame is the last one, tuple belongs to current frame
                }else {
                    //value below or equal to threshold
                    ActiveWindow f = getWindow(fIndex);
                    int index = timestamps.indexOf(position);
                    long timestampAfter = (long) timestamps.get(index + 1);
                    long timestampBefore = (long) timestamps.get(index - 1);
                    long last_ts = f.getEnd();
                    long nextStart = -1;

                    if((fIndex+1) < numberOfActiveWindows()) { // current frame is not the last frame
                        nextStart = getWindow(fIndex + 1).getStart(); // get start of next frame
                    }

                    if (timestampBefore < last_ts && timestampAfter != nextStart) {
                        shiftEndAndModify(f, position);
                        if (timestampAfter != last_ts) { //shift frame end
                            return addNewWindow(fIndex + 1, timestampAfter, last_ts);
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public long assignNextWindowStart(long position) {
           return position;
        }

        public int getFrame(long position) {
            // returns newest frame
            int i = numberOfActiveWindows()-1;
            for (; i >= 0 ; i--) {
                ActiveWindow p = getWindow(i);
                if (p.getStart() <= position) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
            if(numberOfActiveWindows() > 0) {
                ActiveWindow window = getWindow(0);
                while (window.getEnd() <= currentWatermark && window.getEnd() <= lastEnd) {
                    aggregateWindows.trigger(window.getStart(), window.getEnd(), measure);
                    removeWindow(0);
                    if (hasActiveWindows())
                        return;
                    window = getWindow(0);
                }
            }
        }
    }
}
