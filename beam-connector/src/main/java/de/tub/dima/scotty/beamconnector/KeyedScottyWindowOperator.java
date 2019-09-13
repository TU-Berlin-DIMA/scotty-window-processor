package de.tub.dima.scotty.beamconnector;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
 * @author Batuhan Tüter
 * Scotty Window Operation connector for Apache Beam
 *
 */

public class KeyedScottyWindowOperator<K,V> extends DoFn<KV<K, V>, String> {

    private MemoryStateFactory stateFactory;
    private HashMap<K, SlicingWindowOperator<KV<K, V>>> slicingWindowOperatorMap;
    private long lastWatermark;

    private final AggregateFunction windowFunction;
    private final List<Window> windows;
    private long allowedLateness;
    private long watermarkEvictionPeriod = 1000;

    public KeyedScottyWindowOperator(long allowedLateness, AggregateFunction windowFunction) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
        this.allowedLateness = allowedLateness;
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperatorMap = new HashMap<>();
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


    public SlicingWindowOperator<KV<K, V>> initWindowOperator() {
        SlicingWindowOperator<KV<K, V>> slicingWindowOperator = new SlicingWindowOperator<>(stateFactory);
        for (Window window : windows) {
            slicingWindowOperator.addWindowAssigner(window);
        }
        slicingWindowOperator.addAggregation(windowFunction);
        slicingWindowOperator.setMaxLateness(allowedLateness);
        return slicingWindowOperator;
    }

    @ProcessElement
    public void processElement(@Element KV<K, V> input,
                               @Timestamp Instant timestamp,
                               OutputReceiver<String> out) {

        K currentKey = input.getKey();
        if (!slicingWindowOperatorMap.containsKey(currentKey)) {
            slicingWindowOperatorMap.put(currentKey, initWindowOperator());
        }
        SlicingWindowOperator<KV<K, V>> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
        //Since we process the input, input type  of the window function (e.g. SumWindow) must be KV<string,long>
        slicingWindowOperator.processElement(input, timestamp.getMillis());
        processWatermark(timestamp, out);
    }

    private void processWatermark(Instant timestamp, OutputReceiver<String> out) {
        // Every tuple represents a Watermark with its timestamp.
        // A watermark is processed if it is greater than the old watermark, i.e. monotonically increasing.
        // We process watermarks every watermarkEvictionPeriod in event-time
        if (timestamp.getMillis() > lastWatermark + watermarkEvictionPeriod) {
            for (SlicingWindowOperator<KV<K, V>> slicingWindowOperator : this.slicingWindowOperatorMap.values()) {
                List<AggregateWindow> aggregates = slicingWindowOperator.processWatermark(timestamp.getMillis());
                for (AggregateWindow<KV<K, V>> aggregateWindow : aggregates) {
                    out.output(aggregateWindow.toString());
                }
            }
            lastWatermark = timestamp.getMillis();
        }
    }

}
