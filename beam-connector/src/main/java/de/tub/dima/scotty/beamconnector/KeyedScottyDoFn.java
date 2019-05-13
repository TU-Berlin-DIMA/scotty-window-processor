package de.tub.dima.scotty.beamconnector;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class KeyedScottyDoFn extends DoFn<KV<String, Long>, String> {

    private MemoryStateFactory stateFactory;
    private HashMap<String, SlicingWindowOperator<KV<String, Long>>> slicingWindowOperatorMap;
    private long lastWatermark;

    private final AggregateFunction windowFunction;
    private final List<Window> windows;

    public KeyedScottyDoFn(AggregateFunction windowFunction) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
        this.stateFactory = new MemoryStateFactory();
        slicingWindowOperatorMap = new HashMap<>();
    }

    /**
     * Register a new @{@link Window} definition to the ActiveWindow Operator.
     * For example {@link SlidingWindow} or {@link TumblingWindow}
     *
     * @param window the new window definition
     */
    public void addWindow(Window window) {
        windows.add(window);
    }


    public SlicingWindowOperator<KV<String, Long>> initWindowOperator() {
        SlicingWindowOperator<KV<String, Long>> slicingWindowOperator = new SlicingWindowOperator<>(stateFactory);
        for (Window window : windows) {
            slicingWindowOperator.addWindowAssigner(window);
        }
        slicingWindowOperator.addAggregation(windowFunction);
        return slicingWindowOperator;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Long> input,
                               @Timestamp Instant timestamp,
                               OutputReceiver<String> out) {

        String currentKey = input.getKey();
        if (!slicingWindowOperatorMap.containsKey(currentKey)) {
            slicingWindowOperatorMap.put(currentKey, initWindowOperator());
        }
        SlicingWindowOperator<KV<String, Long>> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
        //Since we process the input, input type  of the window function (e.g. SumWindow) must be KV<string,long>
        slicingWindowOperator.processElement(input, timestamp.getMillis());
        processWatermark(timestamp, out);
    }

    //A function that is used to simulate watermark catch
    private void processWatermark(Instant timestamp, OutputReceiver<String> out) {
        //Watermark comes with every element but we simulate the catch every 1 sec. processing time
        long currentWatermark = Instant.now().getMillis();
        long oneSec = Duration.standardSeconds(1).getMillis();

        if (currentWatermark > this.lastWatermark+oneSec) {
            for (SlicingWindowOperator<KV<String, Long>> slicingWindowOperator : this.slicingWindowOperatorMap.values()) {
                List<AggregateWindow> aggregates = slicingWindowOperator.processWatermark(timestamp.getMillis());
                for (AggregateWindow<KV<String, Long>> aggregateWindow : aggregates) {
                        out.output(aggregateWindow.toString());
                }
            }
            this.lastWatermark = currentWatermark;
        }
    }

}
