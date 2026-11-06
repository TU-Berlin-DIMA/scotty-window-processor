package stream.scotty.core.windowType;

import stream.scotty.core.WindowCollector;
import stream.scotty.core.windowType.windowContext.WindowContext;

import java.util.ArrayList;

public class SlideByTupleWindow implements ForwardContextAware {

    private final WindowMeasure measure;
    private final long size;
    private final long slide;

    /**
     * the measure of the Slide-by-tuple Window is time
     * @param size size of the SlideByTuple window in time
     * @param slide window slide step in tuple counts
     */
    public SlideByTupleWindow(long size, long slide){
        if (size <= 0 || slide <= 0) {
            throw new IllegalArgumentException("size and slide of a SlideByTupleWindow must be positive");
        }
        this.measure = WindowMeasure.Time;
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
    public SlideByTupleContext createContext() {
        return new SlideByTupleContext();
    }

    /**
     * Window k (k = 0, 1, 2, ...) starts at the tuple with index k * slide, where tuples are
     * indexed in event-time order, and spans [start, start + size).
     * Tuples between the end of one window and the start of the next belong to no window.
     */
    public class SlideByTupleContext extends WindowContext<Object> {

        private long nextStart = 0; //tuple index at which the next window starts
        private long firstActiveWindowNumber = 0; //sequence number k of getWindow(0)
        private final ArrayList<Long> timestamps = new ArrayList<Long>(); //sorted timestamps of all tuples, index = tuple count

        @Override
        public ActiveWindow updateContext(Object tuple, long position) {

            if (timestamps.isEmpty() || position >= timestamps.get(timestamps.size() - 1)) {
                //processes in-order tuples
                int windowIndex = getWindowIndex(position);
                timestamps.add(position);
                if (timestamps.size() - 1 == nextStart) {
                    //current tuple starts a new window
                    nextStart += slide;
                    return addNewWindow(numberOfActiveWindows(), position, position + size);
                }
                if (windowIndex != -1 && getWindow(windowIndex).getEnd() > position) {
                    //append to active window
                    return getWindow(windowIndex);
                }
                /* Tuple, which does not belong to current window and does not start a new window,
                does not get included in any window instance */
                return null;
            }

            //processes out-of-order tuples
            int insertIndex = upperBound(position);
            timestamps.add(insertIndex, position);

            //all windows starting at a tuple index >= insertIndex now start one tuple earlier in event time
            shiftWindows();

            if (timestamps.size() - 1 >= nextStart) {
                //the inserted tuple moved the tuple with index nextStart into the stream, start a new window there
                long positionOfNewWindow = timestamps.get((int) nextStart);
                nextStart += slide;
                addNewWindow(numberOfActiveWindows(), positionOfNewWindow, positionOfNewWindow + size);
            }
            return null;
        }

        /**
         * Re-aligns the start (and end) of every active window with the tuple at its start index
         * after an out-of-order tuple has been inserted.
         * The old window edges remain slice edges, new edges are added as slice splits,
         * so that slice edges of other windows are never moved.
         */
        private void shiftWindows() {
            for (int i = 0; i < numberOfActiveWindows(); i++) {
                ActiveWindow w = getWindow(i);
                long newStart = timestamps.get((int) ((firstActiveWindowNumber + i) * slide));
                if (newStart != w.getStart()) {
                    shiftStartDontModifySlice(w, newStart);
                    splitSlice(newStart);
                    shiftEnd(w, newStart + size);
                    splitSlice(newStart + size);
                }
            }
        }

        /**
         * @return index of the first timestamp that is greater than position
         */
        private int upperBound(long position) {
            int low = 0;
            int high = timestamps.size();
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (timestamps.get(mid) <= position) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        public int getWindowIndex(long position) {
            //returns newest window
            int i = numberOfActiveWindows()-1;
            for (; i >= 0 ; i--) {
                ActiveWindow w = getWindow(i);
                if (w.getStart() <= position) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public long assignNextWindowStart(long position) {
            if (timestamps.size() == nextStart) {
                // new window starts, create new slice
                return position;
            } else {
                // return next window end
                return getNextWindowEnd(position);
            }
        }

        public long getNextWindowEnd(long position) {
            //returns next window end after position
            long nextWEnd = Long.MAX_VALUE; // no open window: no slice edge needed
            for (int i = numberOfActiveWindows() -1 ; i >= 0 ; i--) {
                ActiveWindow w = getWindow(i);
                if (position >= w.getEnd()) {
                    break;
                }
                nextWEnd = w.getEnd();
            }
            return nextWEnd;
        }

        @Override
        public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
            while (numberOfActiveWindows() > 0 && getWindow(0).getEnd() <= currentWatermark) {
                ActiveWindow w = getWindow(0);
                aggregateWindows.trigger(w.getStart(), w.getEnd(), measure);
                removeWindow(0);
                firstActiveWindowNumber++;
            }
        }
    }


    @Override
    public String toString() {
        return "SlideByTupleWindow{" +
                "measure=" + measure +
                ", size=" + size +
                ", slide=" + slide +
                '}';
    }
}
