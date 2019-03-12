package de.tub.dima.scotty.slicing;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.core.windowType.windowContext.*;
import de.tub.dima.scotty.slicing.aggregationstore.*;
import de.tub.dima.scotty.slicing.state.*;
import de.tub.dima.scotty.state.*;
import org.jetbrains.annotations.*;

import java.util.*;

public class WindowManager {

    private final AggregationStore aggregationStore;
    private final StateFactory stateFactory;
    private boolean hasContextAwareWindows = false;
    private boolean hasFixedWindows;
    private long minSessionTimeout;
    private long maxLateness = 1000;
    private final List<ContextFreeWindow> contextFreeWindows = new ArrayList<>();
    private final List<WindowContext> contextAwareWindows = new ArrayList<>();
    private final List<AggregateFunction> windowFunctions = new ArrayList<>();
    private long lastWatermark = -1;

    public WindowManager(StateFactory stateFactory, AggregationStore aggregationStore) {
        this.stateFactory = stateFactory;
        this.aggregationStore = aggregationStore;
    }


    public List<AggregateWindow> processWatermark(long watermarkTs) {

        if (this.lastWatermark == -1)
            this.lastWatermark = Math.max(0,watermarkTs - maxLateness);

        AggregationWindowCollector windows = new AggregationWindowCollector();
        assignContextFreeWindows(watermarkTs, windows);
        assignContextAwareWindows(watermarkTs, windows);

        long minTs = Long.MAX_VALUE, maxTs = 0;

        for (AggregateWindow aggregateWindow : windows) {
            minTs = Math.min(aggregateWindow.getStartTs(), minTs);
            maxTs = Math.max(aggregateWindow.getEndTs(), maxTs);
        }

        if(!windows.isEmpty()){
            this.aggregationStore.aggregate(windows, minTs, maxTs);
        }
        this.lastWatermark = watermarkTs;
        return windows.aggregationStores;

    }

    private void assignContextAwareWindows(long watermarkTs, AggregationWindowCollector windows) {
        for(WindowContext context: contextAwareWindows){
            context.triggerWindows(windows, lastWatermark, watermarkTs);
        }
    }

    private void assignContextFreeWindows(long watermarkTs, WindowCollector windowCollector) {

        for (ContextFreeWindow window : contextFreeWindows) {
            window.triggerWindows(windowCollector, lastWatermark, watermarkTs);
        }
    }


    public void addWindowAssigner(Window window) {
        if (window instanceof ContextFreeWindow) {
            contextFreeWindows.add((ContextFreeWindow) window);
            hasFixedWindows = true;
        }
        if (window instanceof ForwardContextAware) {
            hasContextAwareWindows = true;
            contextAwareWindows.add(((ForwardContextAware) window).createContext());
        }
        if (window instanceof ForwardContextFree) {
            hasContextAwareWindows = true;
            contextAwareWindows.add(((ForwardContextFree) window).createContext());
        }
    }

    public <InputType, Agg, OutputType> void addAggregation(AggregateFunction<InputType, Agg, OutputType> windowFunction) {
        windowFunctions.add(windowFunction);
    }

    public boolean hasContextAwareWindow() {
        return hasContextAwareWindows;
    }


    public boolean hasFixedWindows() { return this.hasFixedWindows; }

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

    public class AggregationWindowCollector implements WindowCollector, Iterable<AggregateWindow> {

        private final List<AggregateWindow> aggregationStores;


        public void trigger(long start, long end) {
            AggregateWindowState aggWindow = new AggregateWindowState(start, end, stateFactory, windowFunctions);
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

        boolean isEmpty(){
            return this.aggregationStores.isEmpty();
        }
    }
}
