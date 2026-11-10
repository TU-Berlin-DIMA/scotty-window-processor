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
package stream.scotty.kafkastreamsconnector;

import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.AggregateFunction;
import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.Window;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyedScottyWindowOperator<Key, Value> implements Processor<Key,Value> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyedScottyWindowOperator.class);
    private ProcessorContext context;
    private MemoryStateFactory stateFactory;
    private HashMap<Key, SlicingWindowOperator<Value>> slicingWindowOperatorMap;
    private long lastWatermark;
    private final AggregateFunction<Value, ?, ?> windowFunction;
    private final List<Window> windows;
    private long allowedLateness;
    private long watermarkEvictionPeriod = 100;

    public KeyedScottyWindowOperator(AggregateFunction windowFunction, long allowedLateness) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
        this.allowedLateness = allowedLateness;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperatorMap = new HashMap<>();
    }

    @Override
    public void process(Key currentKey, Value value) {
        if (!slicingWindowOperatorMap.containsKey(currentKey)) {
            slicingWindowOperatorMap.put(currentKey, initWindowOperator());
        }
        SlicingWindowOperator<Value> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
        slicingWindowOperator.processElement(value, context.timestamp());
        processWatermark(context.timestamp());
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



    private void processWatermark(long timeStamp) {
        if (timeStamp > lastWatermark + watermarkEvictionPeriod) {
            for (Map.Entry<Key, SlicingWindowOperator<Value>> keyOperatorSet : this.slicingWindowOperatorMap.entrySet()) {
              List<AggregateWindow> aggregates = keyOperatorSet.getValue().processWatermark(timeStamp);
              for (AggregateWindow<Value> aggregateWindow : aggregates) {
                  if (aggregateWindow.hasValue()) {
                      LOG.debug("AggregateWindow: {}", aggregateWindow);
                      for (Value aggValue : aggregateWindow.getAggValues()) {
                          context.forward(keyOperatorSet.getKey(), aggValue);
                      }
                  }
              }
            }
            lastWatermark = timeStamp;
        }
    }

    @Override
    public void close() {

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
