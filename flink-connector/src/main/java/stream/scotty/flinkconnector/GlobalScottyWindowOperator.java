package stream.scotty.flinkconnector;

import stream.scotty.core.windowFunction.*;
import stream.scotty.core.windowType.*;
import stream.scotty.slicing.*;
import stream.scotty.state.memory.*;
import stream.scotty.core.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.*;

/**
 * Scotty window operator for non-keyed streams.
 * Windows are triggered by watermarks, so they are also emitted if no further elements arrive.
 * Apply it with {@link #applyTo(DataStream)} or {@link DataStream#transform}.
 */
public class GlobalScottyWindowOperator<InputType, FinalAggregateType> extends AbstractStreamOperator<AggregateWindow<FinalAggregateType>>
        implements OneInputStreamOperator<InputType, AggregateWindow<FinalAggregateType>> {


    private transient MemoryStateFactory stateFactory;
    private transient SlicingWindowOperator<InputType> slicingWindowOperator;
    private transient TimestampedCollector<AggregateWindow<FinalAggregateType>> collector;
    private long lastWatermark;
    private long currentWatermark = Long.MIN_VALUE;

    private final AggregateFunction<InputType,?, FinalAggregateType> windowFunction;
    private final List<Window> windows;
    private long allowedLateness = 1;

    public GlobalScottyWindowOperator(AggregateFunction<InputType, ?, FinalAggregateType> windowFunction) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }


    @Override
    public void open() throws Exception {
        super.open();
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = initWindowOperator();
        this.collector = new TimestampedCollector<>(output);
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
    public void processElement(StreamRecord<InputType> element) throws Exception {
        long timestamp = getTimestamp(element);
        this.slicingWindowOperator.processElement(element.getValue(), timestamp);

        collector.setTimestamp(element);
        processWatermark(currentWatermark < 0 ? timestamp : currentWatermark);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // trigger windows before the watermark is forwarded (#61)
        currentWatermark = mark.getTimestamp();
        collector.setAbsoluteTimestamp(mark.getTimestamp());
        processWatermark(mark.getTimestamp());
        super.processWatermark(mark);
    }

    private void processWatermark(long currentWaterMark) {
        if (currentWaterMark > this.lastWatermark) {
            List<AggregateWindow> aggregates = this.slicingWindowOperator.processWatermark(currentWaterMark);
            for(AggregateWindow<FinalAggregateType> aggregateWindow: aggregates){
                collector.collect(aggregateWindow);
            }
            this.lastWatermark = currentWaterMark;
        }
    }

    private long getTimestamp(StreamRecord<InputType> element){
        return element.hasTimestamp()?element.getTimestamp():getProcessingTimeService().getCurrentProcessingTime();
    }

    /**
     * Register a new @{@link Window} definition to the ActiveWindow Operator.
     * For example {@link SlidingWindow} or {@link TumblingWindow}
     * @param window the new window definition
     */
    public GlobalScottyWindowOperator<InputType, FinalAggregateType> addWindow(Window window) {
        windows.add(window);
        return this;
    }


    public GlobalScottyWindowOperator<InputType, FinalAggregateType> allowedLateness(Time time){
        this.allowedLateness = time.toMilliseconds();
        return this;
    }

    /**
     * Applies this window operator to a non-keyed stream.
     * @param stream the input stream
     * @return the stream of window aggregates
     */
    @SuppressWarnings("unchecked")
    public SingleOutputStreamOperator<AggregateWindow<FinalAggregateType>> applyTo(DataStream<InputType> stream) {
        TypeInformation<AggregateWindow<FinalAggregateType>> outputType =
                TypeInformation.of((Class<AggregateWindow<FinalAggregateType>>) (Class<?>) AggregateWindow.class);
        return stream.transform("GlobalScottyWindowOperator", outputType, this);
    }
}
