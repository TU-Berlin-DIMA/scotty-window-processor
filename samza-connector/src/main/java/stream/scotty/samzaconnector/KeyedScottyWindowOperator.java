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
package stream.scotty.samzaconnector;

import stream.scotty.core.*;
import stream.scotty.core.windowFunction.*;
import stream.scotty.core.windowType.*;
import stream.scotty.slicing.*;
import stream.scotty.state.memory.*;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class KeyedScottyWindowOperator<Key, Value> implements StreamTask {

    private static final Logger LOG = LoggerFactory.getLogger(KeyedScottyWindowOperator.class);
    private MemoryStateFactory stateFactory;
    private HashMap<Key, SlicingWindowOperator<Value>> slicingWindowOperatorMap;
    private long lastWatermark;
    private final AggregateFunction windowFunction;
    private final List<Window> windows;
    private long allowedLateness;
    private long watermarkEvictionPeriod = 100;
    private SystemStream outputStream;

    public KeyedScottyWindowOperator(AggregateFunction windowFunction, long allowedLateness, SystemStream outputStream) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
        this.allowedLateness = allowedLateness;
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperatorMap = new HashMap<>();
        this.outputStream = outputStream;
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
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Key currentKey = (Key) envelope.getKey();
        if (!slicingWindowOperatorMap.containsKey(currentKey)) {
            slicingWindowOperatorMap.put(currentKey, initWindowOperator());
        }
        SlicingWindowOperator<Value> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
        slicingWindowOperator.processElement((Value) envelope.getMessage(), envelope.getEventTime());
        processWatermark(envelope.getEventTime(), collector);
    }

    private void processWatermark(long timeStamp, MessageCollector collector) {
        if (timeStamp > lastWatermark + watermarkEvictionPeriod) {
            for (SlicingWindowOperator<Value> slicingWindowOperator : this.slicingWindowOperatorMap.values()) {
                List<AggregateWindow> aggregates = slicingWindowOperator.processWatermark(timeStamp);
                for (AggregateWindow<Value> aggregateWindow : aggregates) {
                    if (aggregateWindow.hasValue()) {
                        LOG.debug("AggregateWindow: {}", aggregateWindow);
                        for (Value aggValue : aggregateWindow.getAggValues()) {
                            collector.send(new OutgoingMessageEnvelope(outputStream, aggValue));
                        }

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
