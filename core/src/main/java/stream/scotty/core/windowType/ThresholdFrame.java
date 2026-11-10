package stream.scotty.core.windowType;

import stream.scotty.core.WindowCollector;
import stream.scotty.core.windowType.windowContext.WindowContext;
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
        private ArrayList<Long> timestamps = new ArrayList<Long>(); //holds sorted timestamps of tuples for out-of-order processing
        private long prunedUpTo = Long.MIN_VALUE; //timestamps below this bound were removed from timestamps

        @Override
        public ActiveWindow updateContext(Object o, long position) {
            int value;
            if(o instanceof Tuple){ //fetching the value of the attribute in the tuple
                value = (int)((Tuple) o).getField(attribute);
            }else{
                value = (int)o;
            }

            if (position < prunedUpTo) {
                //very late tuple before all active frames, its neighbours are no longer known
                return null;
            }

            if (hasActiveWindows()) {
                //no active frames, record the timestamp so that out-of-order tuples see all neighbours
                insertTimestamp(position);
                if(value > threshold){ //begin of first frame
                    count++;
                    gap = 0;
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
                int index = insertTimestamp(position);

                if(value > threshold){
                    if((fIndex+1) < numberOfActiveWindows()) { //frame after this one exists
                        long timestampAfter = (long) timestamps.get(index + 1);
                        ActiveWindow fNext = getWindow(fIndex + 1);
                        if (timestampAfter == fNext.getStart()) { //shift frame start of next window to current tuple
                            shiftStart(fNext, position);
                            return fNext;
                        }
                        //else: simple insert, changes nothing
                    }
                    //else: the tuple lies within a frame or in a gap; a single tuple in a gap does not form a frame
                }else if (fIndex != -1) {
                    //value below or equal to threshold, tuples before the first frame change nothing
                    ActiveWindow f = getWindow(fIndex);
                    long timestampAfter = (long) timestamps.get(index + 1);
                    long timestampBefore = index > 0 ? (long) timestamps.get(index - 1) : Long.MIN_VALUE;
                    long last_ts = f.getEnd();
                    long nextStart = -1;
                    //the last frame is still open while in-order tuples are above the threshold;
                    //its end is then the timestamp of its last tuple, which belongs to the frame
                    boolean isOpen = (fIndex + 1) == numberOfActiveWindows() && count > 0;

                    if((fIndex+1) < numberOfActiveWindows()) { // current frame is not the last frame
                        nextStart = getWindow(fIndex + 1).getStart(); // get start of next frame
                    }

                    if (timestampBefore < last_ts && timestampAfter != nextStart) {
                        //the frame now ends at this tuple; the old end edge stays, as it may end the split-off part
                        shiftEndAndAddEdge(f, position);
                        lastEnd = Math.max(lastEnd, position);
                        if (isOpen) {
                            //the tuples after this one continue in a new open frame
                            count = timestamps.size() - index - 1;
                            return addNewWindow(fIndex + 1, timestampAfter, last_ts);
                        }
                        if (timestampAfter != last_ts) { //split closed frame, last_ts is its closing tuple
                            return addNewWindow(fIndex + 1, timestampAfter, last_ts);
                        }
                    }
                }
            }
            return null;
        }

        /**
         * Inserts a timestamp into the sorted list of timestamps.
         * @return the index of the inserted timestamp
         */
        private int insertTimestamp(long position) {
            int index = Collections.binarySearch(timestamps, position);
            if (index < 0) {
                index = -index - 1;
            }
            timestamps.add(index, position);
            return index;
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
        public boolean hasPendingWindows() {
            // the frame that is still open is only triggered after a tuple below the threshold closed it
            return numberOfActiveWindows() > 0 && getWindow(0).getEnd() <= lastEnd;
        }

        @Override
        public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
            while (numberOfActiveWindows() > 0) {
                ActiveWindow window = getWindow(0);
                if (window.getEnd() > currentWatermark || window.getEnd() > lastEnd) {
                    break;
                }
                aggregateWindows.trigger(window.getStart(), window.getEnd(), measure);
                removeWindow(0);
            }
            pruneTimestamps(currentWatermark);
        }

        /**
         * Removes timestamps that can no longer influence an active frame, so that the
         * context does not grow with the stream. Keeps everything from the start of the
         * oldest active frame (or the watermark) on, and always the latest timestamp.
         */
        private void pruneTimestamps(long currentWatermark) {
            if (timestamps.isEmpty()) {
                return;
            }
            long bound = Math.min(currentWatermark, timestamps.get(timestamps.size() - 1));
            if (numberOfActiveWindows() > 0) {
                bound = Math.min(bound, getWindow(0).getStart());
            }
            int index = Collections.binarySearch(timestamps, bound);
            if (index < 0) {
                index = -index - 1;
            }
            while (index > 0 && timestamps.get(index - 1) == bound) {
                index--;
            }
            if (index > 0) {
                timestamps.subList(0, index).clear();
                prunedUpTo = Math.max(prunedUpTo, bound);
            }
        }
    }
}
