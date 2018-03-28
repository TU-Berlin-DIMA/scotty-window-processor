package de.tub.dima.scotty.flinkconnector;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.state.memory.*;
import de.tub.dima.scotty.core.*;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.functions.*;
import org.apache.flink.util.*;

import java.util.*;

public class GlobalScottyWindowOperator<InputType, FinalAggregateType> extends ProcessFunction<InputType, AggregateWindow<FinalAggregateType>> {


    private MemoryStateFactory stateFactory;
    private SlicingWindowOperator<InputType> slicingWindowOperator;
    private long lastWatermark;

    private final AggregateFunction<InputType,?, FinalAggregateType> windowFunction;
    private final List<Window> windows;

    public GlobalScottyWindowOperator(AggregateFunction<InputType, ?, FinalAggregateType> windowFunction) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = initWindowOperator();
    }

    public SlicingWindowOperator initWindowOperator(){
        SlicingWindowOperator<InputType> slicingWindowOperator = new SlicingWindowOperator<>(stateFactory);
        for(Window window: windows){
            slicingWindowOperator.addWindowAssigner(window);
        }
        slicingWindowOperator.addAggregation(windowFunction);
        return slicingWindowOperator;
    }

    @Override
    public void processElement(InputType value, Context ctx, Collector<AggregateWindow<FinalAggregateType>> out) throws Exception {

        this.slicingWindowOperator.processElement(value, ctx.timestamp());

        long currentWaterMark = ctx.timerService().currentWatermark();

        if (currentWaterMark > this.lastWatermark) {
            List<AggregateWindow> aggregates = this.slicingWindowOperator.processWatermark(currentWaterMark);
            for(AggregateWindow<FinalAggregateType> aggregateWindow: aggregates){
                out.collect(aggregateWindow);
            }
            this.lastWatermark = currentWaterMark;
        }
    }

    /**
     * Register a new @{@link Window} definition to the ActiveWindow Operator.
     * For example {@link SlidingWindow} or {@link TumblingWindow}
     * @param window the new window definition
     */
    public void addWindow(Window window) {
        windows.add(window);
    }

}
