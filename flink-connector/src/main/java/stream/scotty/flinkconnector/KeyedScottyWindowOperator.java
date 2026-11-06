package stream.scotty.flinkconnector;

import stream.scotty.core.*;
import stream.scotty.core.windowFunction.*;
import stream.scotty.core.windowType.*;
import stream.scotty.slicing.*;
import stream.scotty.state.memory.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.*;
import org.apache.flink.util.*;

import java.util.*;

public class KeyedScottyWindowOperator<Key, InputType, FinalAggregateType> extends KeyedProcessFunction<Key, InputType, AggregateWindow<FinalAggregateType>> {


    private MemoryStateFactory stateFactory;
    private HashMap<Key, SlicingWindowOperator<InputType>> slicingWindowOperatorMap;
    // max event timestamp per key, used to stop re-registering watermark timers for idle keys
    private HashMap<Key, Long> maxTimestampMap;
    private long lastWatermark;

    private final AggregateFunction<InputType, ?, FinalAggregateType> windowFunction;
    private final List<Window> windows;
    private long allowedLateness = 1;

    public KeyedScottyWindowOperator(AggregateFunction<InputType, ?, FinalAggregateType> windowFunction) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.stateFactory = new MemoryStateFactory();
        slicingWindowOperatorMap = new HashMap<>();
        maxTimestampMap = new HashMap<>();
    }

    public SlicingWindowOperator<InputType> initWindowOperator() {
        SlicingWindowOperator<InputType> slicingWindowOperator = new SlicingWindowOperator<>(stateFactory);
        for (Window window : windows) {
            slicingWindowOperator.addWindowAssigner(window);
        }
        slicingWindowOperator.addAggregation(windowFunction);
        slicingWindowOperator.setMaxLateness(allowedLateness);
        return slicingWindowOperator;
    }

    public Key getKey(Context ctx) {
        return ctx.getCurrentKey();
    }

    @Override
    public void processElement(InputType value, Context ctx, Collector<AggregateWindow<FinalAggregateType>> out) throws Exception {
        Key currentKey = getKey(ctx);
        if (!slicingWindowOperatorMap.containsKey(currentKey)) {
            slicingWindowOperatorMap.put(currentKey, initWindowOperator());
        }
        SlicingWindowOperator<InputType> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
        long timestamp = getTimestamp(ctx);
        slicingWindowOperator.processElement(value, timestamp);
        maxTimestampMap.merge(currentKey, timestamp, Math::max);
        processWatermark(ctx, out);
        // A KeyedProcessFunction is not notified about watermarks. Register an event-time timer that fires
        // with the next watermark, so that windows are also triggered if no further elements arrive (#61).
        registerWatermarkTimer(ctx.timerService().currentWatermark(), ctx.timerService());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregateWindow<FinalAggregateType>> out) throws Exception {
        long currentWatermark = ctx.timerService().currentWatermark();
        processWatermark(currentWatermark, out);
        Long maxTimestamp = maxTimestampMap.get(ctx.getCurrentKey());
        if (maxTimestamp != null && currentWatermark <= maxTimestamp + getTriggerHorizon()) {
            registerWatermarkTimer(currentWatermark, ctx.timerService());
        }
    }

    private void registerWatermarkTimer(long currentWatermark, TimerService timerService) {
        if (currentWatermark < Long.MAX_VALUE) {
            timerService.registerEventTimeTimer(currentWatermark + 1);
        }
    }

    /**
     * How long after the last element of a key the watermark timers of that key are kept alive.
     */
    private long getTriggerHorizon() {
        long horizon = 0;
        for (Window window : windows) {
            if (window instanceof ContextFreeWindow) {
                horizon = Math.max(horizon, ((ContextFreeWindow) window).clearDelay());
            } else {
                // context aware windows (e.g. session or punctuation windows) have no fixed size
                return Long.MAX_VALUE / 2;
            }
        }
        return horizon + allowedLateness;
    }

    private long getTimestamp(Context context){
        return context.timestamp()!=null?context.timestamp():context.timerService().currentProcessingTime();
    }

    private void processWatermark(Context ctx, Collector<AggregateWindow<FinalAggregateType>> out) {
        long currentWaterMark = ctx.timerService().currentWatermark()<0?getTimestamp(ctx):ctx.timerService().currentWatermark();
        processWatermark(currentWaterMark, out);
    }

    private void processWatermark(long currentWaterMark, Collector<AggregateWindow<FinalAggregateType>> out) {
        if (currentWaterMark > this.lastWatermark) {
            for (SlicingWindowOperator<InputType> slicingWindowOperator : this.slicingWindowOperatorMap.values()) {
                List<AggregateWindow> aggregates = slicingWindowOperator.processWatermark(currentWaterMark);
                for (AggregateWindow<FinalAggregateType> aggregateWindow : aggregates) {
                    if(aggregateWindow.hasValue())
                        out.collect(aggregateWindow);
                }
            }
            this.lastWatermark = currentWaterMark;
        }
    }

    /**
     * Register a new @{@link Window} definition to the ActiveWindow Operator.
     * For example {@link SlidingWindow} or {@link TumblingWindow}
     * @param window the new window definition
     */
    public KeyedScottyWindowOperator addWindow(Window window) {
        windows.add(window);
        return this;
    }

    public KeyedScottyWindowOperator allowedLateness(Time time){
        this.allowedLateness = time.toMilliseconds();
        return this;
    }

}
