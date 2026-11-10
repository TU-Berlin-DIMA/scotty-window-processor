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
        // A KeyedProcessFunction is not notified about watermarks. Register an event-time timer that fires
        // with the next watermark, so that windows are also triggered if no further elements arrive (#61).
        registerWatermarkTimer(ctx.timerService().currentWatermark(), ctx.timerService());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregateWindow<FinalAggregateType>> out) throws Exception {
        long currentWatermark = ctx.timerService().currentWatermark();
        processWatermark(currentWatermark, out);
        // Stop registering timers for this key if only new elements can trigger further windows of the key.
        // The next element of the key registers a new timer.
        SlicingWindowOperator<InputType> slicingWindowOperator = slicingWindowOperatorMap.get(ctx.getCurrentKey());
        if (slicingWindowOperator != null && slicingWindowOperator.hasPendingWindows()) {
            registerWatermarkTimer(currentWatermark, ctx.timerService());
        }
    }

    private void registerWatermarkTimer(long currentWatermark, TimerService timerService) {
        if (currentWatermark < Long.MAX_VALUE) {
            timerService.registerEventTimeTimer(currentWatermark + 1);
        }
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
