package de.tub.dima.scotty.sparkconnector;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.sparkconnector.demo.DemoEvent;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class KeyedScottyWindowOperator<Key, Value> implements FlatMapFunction<DemoEvent, Value> {
    private MemoryStateFactory stateFactory;
    private HashMap<Key, SlicingWindowOperator<Value>> slicingWindowOperatorMap;
    private long lastWatermark;
    private AggregateFunction windowFunction;
    private List<Window> windows;
    private long allowedLateness;
    private long watermarkEvictionPeriod = 100;

    public KeyedScottyWindowOperator(AggregateFunction windowFunction, long allowedLateness) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
        this.allowedLateness = allowedLateness;
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperatorMap = new HashMap<>();
    }


    public SlicingWindowOperator<Value> initWindowOperator() {
        SlicingWindowOperator<Value> slicingWindowOperator = new SlicingWindowOperator<>(stateFactory);
        for (Window window : windows) {
            slicingWindowOperator.addWindowAssigner(window);
        }
        slicingWindowOperator.addAggregation(windowFunction);
        slicingWindowOperator.setMaxLateness(allowedLateness);
        return slicingWindowOperator;
    }

    @Override
    public Iterator<Value> call(DemoEvent demoEvent) throws Exception {
        Key currentKey = (Key) demoEvent.getKey();
        if (!slicingWindowOperatorMap.containsKey(currentKey)) {
            slicingWindowOperatorMap.put(currentKey, initWindowOperator());
        }
        SlicingWindowOperator<Value> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
        slicingWindowOperator.processElement((Value) demoEvent.getValue(), demoEvent.getTimestamp());
        ArrayList<Value> output = new ArrayList<>();
        processWatermark(demoEvent.getTimestamp(), currentKey, output);

        return output.iterator();
    }

    private void processWatermark(long timeStamp, Key currentKey, ArrayList<Value> outputList) {
        if (timeStamp > lastWatermark + watermarkEvictionPeriod) {
            for (SlicingWindowOperator<Value> slicingWindowOperator : this.slicingWindowOperatorMap.values()) {
                List<AggregateWindow> aggregates = slicingWindowOperator.processWatermark(timeStamp);
                for (AggregateWindow<Value> aggregateWindow : aggregates) {
                    if (aggregateWindow.hasValue()) {
                        System.out.println("AggregateWindow: "+aggregateWindow);
                        outputList.addAll(aggregateWindow.getAggValues());
                    }
                }
            }
            lastWatermark = timeStamp;
        }
    }


    /**
     * Register a new @{@link Window} definition to the ActiveWindow Operator.
     * For example {@link SlidingWindow} or {@link TumblingWindow}
     *
     * @param window the new window definition
     */
    public KeyedScottyWindowOperator addWindow(Window window) {
        windows.add(window);
        return this;
    }
}