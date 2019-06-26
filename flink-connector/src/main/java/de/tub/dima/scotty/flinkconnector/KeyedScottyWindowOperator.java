package de.tub.dima.scotty.flinkconnector;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.state.memory.*;
import de.tub.dima.scotty.core.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.functions.*;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.*;

import java.util.*;

public class KeyedScottyWindowOperator<Key, InputType, FinalAggregateType> extends KeyedProcessFunction<Key, InputType, AggregateWindow<FinalAggregateType>> {


    private MemoryStateFactory stateFactory;
    private HashMap<Key, SlicingWindowOperator<InputType>> slicingWindowOperatorMap;
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
        slicingWindowOperator.processElement(value, getTimestamp(ctx));
        processWatermark(ctx, out);


    }

    private long getTimestamp(Context context){
        return context.timestamp()!=null?context.timestamp():context.timerService().currentProcessingTime();
    }

    private void processWatermark(Context ctx, Collector<AggregateWindow<FinalAggregateType>> out) {

        long currentWaterMark = ctx.timerService().currentWatermark()<0?getTimestamp(ctx):ctx.timerService().currentWatermark();

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
