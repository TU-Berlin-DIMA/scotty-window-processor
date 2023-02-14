package stream.scotty.slicing;

import stream.scotty.core.*;
import stream.scotty.core.windowFunction.*;
import stream.scotty.core.windowType.*;
import static stream.scotty.core.windowType.WindowMeasure.*;
import stream.scotty.core.windowType.windowContext.*;
import stream.scotty.slicing.aggregationstore.*;
import stream.scotty.slicing.slice.*;
import stream.scotty.slicing.state.*;
import stream.scotty.state.*;
import org.jetbrains.annotations.*;

import java.util.*;

public class WindowManager {

    private final AggregationStore aggregationStore;
    private final StateFactory stateFactory;
    private boolean hasContextAwareWindows = false;
    private boolean hasFixedWindows;
    private boolean hasCountMeasure;
    private long minSessionTimeout;
    private long maxLateness = 1000;
    private long maxFixedWindowSize = 0;
    private final List<ContextFreeWindow> contextFreeWindows = new ArrayList<>();
    private final List<WindowContext> contextAwareWindows = new ArrayList<>();
    private final List<AggregateFunction> windowFunctions = new ArrayList<>();
    private long lastWatermark = -1;
    private boolean hasTimeMeasure;
    private long currentCount = 0;
    private long lastCount = 0;
    private boolean isSessionWindowCase;
    private long minAllowedTimestamp = Long.MAX_VALUE;  
    private boolean resendWindowsInAllowedLateness;

    public WindowManager(StateFactory stateFactory, AggregationStore aggregationStore) {
        this.stateFactory = stateFactory;
        this.aggregationStore = aggregationStore;
    }


    public List<AggregateWindow> processWatermark(long watermarkTs) {

        if (this.lastWatermark == -1)
            this.lastWatermark = Math.max(0, watermarkTs - maxLateness);

        if (this.aggregationStore.isEmpty()) {
            this.lastWatermark = watermarkTs;
            return new ArrayList<>();
        }

        long oldestSliceStart = this.aggregationStore.getSlice(0).getTStart();

        if (this.lastWatermark < oldestSliceStart) {
            this.lastWatermark = oldestSliceStart;
        }

        AggregationWindowCollector windows = new AggregationWindowCollector();
        assignContextFreeWindows(watermarkTs, windows);
        assignContextAwareWindows(watermarkTs, windows);

        long minTs = Long.MAX_VALUE, maxTs = 0, minCount = currentCount, maxCount = 0;

        for (AggregateWindow aggregateWindow : windows) {
            if (aggregateWindow.getMeasure() == Time) {
                minTs = Math.min(aggregateWindow.getStart(), minTs);
                maxTs = Math.max(aggregateWindow.getEnd(), maxTs);
            } else if (aggregateWindow.getMeasure() == Count) {
                minCount = Math.min(aggregateWindow.getStart(), minCount);
                maxCount = Math.max(aggregateWindow.getEnd(), maxCount);
            }
        }

        if (!windows.isEmpty()) {
            this.aggregationStore.aggregate(windows, minTs, maxTs, minCount, maxCount);
        }
        this.lastWatermark = watermarkTs;
        this.lastCount = currentCount;
        clearAfterWatermark(watermarkTs - maxLateness);
        return windows.aggregationStores;
    }

    public void clearAfterWatermark(long currentWatermark) {

        long firstActiveWindowStart = currentWatermark;
        for (WindowContext<?> context : contextAwareWindows) {
           for(WindowContext.ActiveWindow window : context.getActiveWindows()){
               firstActiveWindowStart = Math.min(firstActiveWindowStart, window.getStart());
           }
        }

        long maxDelay =  currentWatermark - maxFixedWindowSize;
        long removeFromTimestamp = Math.min(maxDelay, firstActiveWindowStart);
        this.minAllowedTimestamp = removeFromTimestamp;

        this.aggregationStore.removeSlices(removeFromTimestamp);
    }


    private void assignContextAwareWindows(long watermarkTs, AggregationWindowCollector windows) {
        for (WindowContext context : contextAwareWindows) {
            context.triggerWindows(windows, lastWatermark, watermarkTs);
        }
    }

    private void assignContextFreeWindows(long watermarkTs, WindowCollector windowCollector) {

        for (ContextFreeWindow window : contextFreeWindows) {
            if (window.getWindowMeasure() == Time)
                window.triggerWindows(windowCollector, lastWatermark, watermarkTs);
            else if (window.getWindowMeasure() == Count) {
                int sliceIndex = this.aggregationStore.findSliceIndexByTimestamp(watermarkTs);
                Slice slice = this.aggregationStore.getSlice(sliceIndex);
                if (slice.getTLast() >= watermarkTs && sliceIndex > 0)
                    slice = this.aggregationStore.getSlice(sliceIndex - 1);
                long cend = slice.getCLast();
                window.triggerWindows(windowCollector, lastCount, cend + 1);
            }
        }
    }


    public void addWindowAssigner(Window window) {
        if (window instanceof ContextFreeWindow) {
            contextFreeWindows.add((ContextFreeWindow) window);
            maxFixedWindowSize = Math.max(maxFixedWindowSize, ((ContextFreeWindow) window).clearDelay());
            hasFixedWindows = true;
        }
        if (window instanceof ForwardContextAware) {

            if(window instanceof SessionWindow && (!hasContextAwareWindows || isSessionWindowCase)){
                isSessionWindowCase = true;
            } else {
                isSessionWindowCase = false;
            }

            hasContextAwareWindows = true;
            contextAwareWindows.add(((ForwardContextAware) window).createContext());
        }
        if (window instanceof ForwardContextFree) {
            hasContextAwareWindows = true;
            contextAwareWindows.add(((ForwardContextFree) window).createContext());
        }
        if (window.getWindowMeasure() == Count) {
            hasCountMeasure = true;
        } else {
            hasTimeMeasure = true;
        }
    }

    public <InputType, Agg, OutputType> void addAggregation(AggregateFunction<InputType, Agg, OutputType> windowFunction) {
        windowFunctions.add(windowFunction);
    }

    public boolean hasContextAwareWindow() {
        return hasContextAwareWindows;
    }


    public boolean hasFixedWindows() {
        return this.hasFixedWindows;
    }

    public long getMinSessionTimeout() {
        return minSessionTimeout;
    }

    public long getMaxLateness() {
        return maxLateness;
    }

    public List<ContextFreeWindow> getContextFreeWindows() {
        return contextFreeWindows;
    }

    public List<AggregateFunction> getAggregations() {
        return Collections.unmodifiableList(windowFunctions);
    }

    public List<? extends WindowContext> getContextAwareWindows() {
        return this.contextAwareWindows;
    }

    public boolean hasCountMeasure() {
        return hasCountMeasure;
    }

    public boolean hasTimeMeasure() {
        return hasTimeMeasure;
    }

    public boolean isSessionWindowCase() {return isSessionWindowCase; }

    public long getCurrentCount() {
        return currentCount;
    }

    public void incrementCount() {
        currentCount++;
    }

    public void setMaxLateness(long maxLateness) {
        this.maxLateness = maxLateness;
    }
  
    public long getMinAllowedTimestamp() { return this.minAllowedTimestamp; }

    public void setMinAllowedTimestamp(long minAllowedTimestamp) { this.minAllowedTimestamp = minAllowedTimestamp; }

    public long getLastWatermark() { return this.lastWatermark; }

    public void setLastWatermarkToAllowedLateness() { this.lastWatermark = this.lastWatermark - this.maxLateness; }

    public void setResendWindowsInAllowedLateness(boolean resendWindowsInAllowedLateness) {this.resendWindowsInAllowedLateness = resendWindowsInAllowedLateness;}

    public boolean getResendWindowsInAllowedLateness() {return this.resendWindowsInAllowedLateness;}

    public class AggregationWindowCollector implements WindowCollector, Iterable<AggregateWindow> {

        private final List<AggregateWindow> aggregationStores;


        public void trigger(long start, long end, WindowMeasure measure) {
            AggregateWindowState aggWindow = new AggregateWindowState(start, end, measure, stateFactory, windowFunctions);
            this.aggregationStores.add(aggWindow);
        }

        public AggregationWindowCollector() {
            this.aggregationStores = new ArrayList<>();
        }

        @NotNull
        @Override
        public Iterator<AggregateWindow> iterator() {
            return aggregationStores.iterator();
        }

        boolean isEmpty() {
            return this.aggregationStores.isEmpty();
        }
    }
}
