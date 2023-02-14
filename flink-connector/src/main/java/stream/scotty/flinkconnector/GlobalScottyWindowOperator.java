package stream.scotty.flinkconnector;

import stream.scotty.core.windowFunction.*;
import stream.scotty.core.windowType.*;
import stream.scotty.slicing.*;
import stream.scotty.state.memory.*;
import stream.scotty.core.*;
import org.apache.flink.api.common.time.Time;
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
    private long allowedLateness = 1;

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
        slicingWindowOperator.setMaxLateness(allowedLateness);
        return slicingWindowOperator;
    }

    @Override
    public void processElement(InputType value, Context ctx, Collector<AggregateWindow<FinalAggregateType>> out) throws Exception {

        this.slicingWindowOperator.processElement(value, getTimestamp(ctx));

        long currentWaterMark = ctx.timerService().currentWatermark()<0?getTimestamp(ctx):ctx.timerService().currentWatermark();

        if (currentWaterMark > this.lastWatermark) {
            List<AggregateWindow> aggregates = this.slicingWindowOperator.processWatermark(currentWaterMark);
            for(AggregateWindow<FinalAggregateType> aggregateWindow: aggregates){
                out.collect(aggregateWindow);
            }
            this.lastWatermark = currentWaterMark;
        }
    }

    private long getTimestamp(Context context){
        return context.timestamp()!=null?context.timestamp():context.timerService().currentProcessingTime();
    }

    /**
     * Register a new @{@link Window} definition to the ActiveWindow Operator.
     * For example {@link SlidingWindow} or {@link TumblingWindow}
     * @param window the new window definition
     */
    public GlobalScottyWindowOperator addWindow(Window window) {
        windows.add(window);
        return this;
    }


    public GlobalScottyWindowOperator allowedLateness(Time time){
        this.allowedLateness = time.toMilliseconds();
        return this;
    }
}
