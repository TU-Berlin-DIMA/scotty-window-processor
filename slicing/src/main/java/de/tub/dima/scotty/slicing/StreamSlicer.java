package de.tub.dima.scotty.slicing;

import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.core.windowType.windowContext.*;
import de.tub.dima.scotty.slicing.slice.*;

public class StreamSlicer {

    private final SliceManager<?> sliceManager;
    private final WindowManager windowManager;
    private long maxEventTime = Long.MIN_VALUE;
    //private long min_next_edge_ts =  Long.MIN_VALUE;
    private long min_next_edge_ts = Long.MIN_VALUE;
    private long min_next_edge_count = Long.MIN_VALUE;

    public StreamSlicer(SliceManager<?> sliceManager, WindowManager windowManager) {
        this.sliceManager = sliceManager;
        this.windowManager = windowManager;
    }

    private class Edge {
        private final long timeStamp;
        private final Slice.Type type;

        private Edge(long timeStamp, Slice.Type type) {
            this.timeStamp = timeStamp;
            this.type = type;
        }
    }

    /**
     * Processes every tuple in the data stream and checks if new slices have to be created
     *
     * @param te event timestamp for which slices have to be created
     */
    public void determineSlices(final long te) {
        if (this.windowManager.hasCountMeasure()) {
            if (this.min_next_edge_count == Long.MIN_VALUE || windowManager.getCurrentCount() == this.min_next_edge_count) {
                if(maxEventTime == Long.MIN_VALUE)
                    maxEventTime = te;
                sliceManager.appendSlice(maxEventTime, new Slice.Fixed());
                this.min_next_edge_count = calculateNextFixedEdgeCount();
            }
        }


        if (this.windowManager.hasTimeMeasure()) {
            boolean isInOrder = this.isInOrder(te);

            // We only need to check for slices if the record is in order
            if (isInOrder) {

                if (this.windowManager.hasFixedWindows() && this.min_next_edge_ts == Long.MIN_VALUE) {
                    this.min_next_edge_ts = calculateNextFixedEdge(te);
                }

                int flex_count = 0;
                if (this.windowManager.hasContextAwareWindow()) {
                    flex_count = calculateNextFlexEdge(te);
                    //(maxEventTime == Long.MIN_VALUE  || te >= min_flex_edge);

                }

                // Tumbling and Sliding windows
                while (windowManager.hasFixedWindows() && te > min_next_edge_ts) {
                    if (min_next_edge_ts >= 0)
                        sliceManager.appendSlice(min_next_edge_ts, new Slice.Fixed());
                    min_next_edge_ts = calculateNextFixedEdge(te);
                }

                // Emit remaining separator if needed
                if (min_next_edge_ts == te) {
                    if (flex_count > 0) {
                        sliceManager.appendSlice(te, new Slice.Fixed());
                    } else {
                        sliceManager.appendSlice(min_next_edge_ts, new Slice.Fixed());
                    }
                    min_next_edge_ts = calculateNextFixedEdge(te);
                } else if (flex_count > 0) {
                    sliceManager.appendSlice(te, new Slice.Flexible(flex_count));
                }
            }
        }
        windowManager.incrementCount();
        maxEventTime = Math.max(te, maxEventTime);
    }

    private long calculateNextFixedEdgeCount() {
        // next_edge will be the last edge
        long current_min_edge = min_next_edge_count == Long.MIN_VALUE ? 0 : min_next_edge_count;
        long t_c = Math.max(windowManager.getCurrentCount(), current_min_edge);
        long edge = Long.MAX_VALUE;
        for (ContextFreeWindow tw : this.windowManager.getContextFreeWindows()) {
            if (tw.getWindowMeasure() == WindowMeasure.Count) {
                //long newNextEdge = t_c + tw.getSize() - (t_c) % tw.getSize();
                long newNextEdge = tw.assignNextWindowStart(t_c);
                edge = Math.min(newNextEdge, edge);
            }
        }
        return edge;
    }

    private long calculateNextFixedEdge(long te) {
        // next_edge will be the last edge
        long current_min_edge = min_next_edge_ts == Long.MIN_VALUE ? Long.MAX_VALUE : min_next_edge_ts;
        long t_c = Math.max(te - this.windowManager.getMaxLateness(), current_min_edge);
        long edge = Long.MAX_VALUE;
        for (ContextFreeWindow tw : this.windowManager.getContextFreeWindows()) {
            if (tw.getWindowMeasure() == WindowMeasure.Time) {
                //long newNextEdge = t_c + tw.getSize() - (t_c) % tw.getSize();
                long newNextEdge = tw.assignNextWindowStart(t_c);
                edge = Math.min(newNextEdge, edge);
            }
        }
        return edge;
    }

    private int calculateNextFlexEdge(long te) {
        // next_edge will be the last edge
        long t_c = Math.max(this.maxEventTime, min_next_edge_ts);
        long edge = Long.MAX_VALUE;
        int flex_count = 0;
        for (WindowContext cw : this.windowManager.getContextAwareWindows()) {
            //long newNextEdge = t_c + tw.getSize() - (t_c) % tw.getSize();
            long newNextEdge = cw.assignNextWindowStart(t_c);
            if (te >= newNextEdge)
                flex_count++;
        }
        return flex_count;
    }


    /**
     * Checks if timestamp is >= @maxEventTime
     *
     * @param te event timestamp
     * @return boolean
     */
    private boolean isInOrder(long te) {
        return te >= this.maxEventTime;
    }

}
