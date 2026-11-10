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
package stream.scotty.stormconnector;

import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.AggregateFunction;
import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.Window;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * @author Batuhan Tüter
 * Scotty Window Operation connector for Apache Storm
 *
 *
 * Input data should be in the following format:
 * <key,value,timeStamp>, timeStamp is type Long
 * */

public class KeyedScottyWindowOperator<Key, Value> extends BaseBasicBolt {

    private MemoryStateFactory stateFactory;
    private HashMap<Key, SlicingWindowOperator<Value>> slicingWindowOperatorMap;
    private long lastWatermark;
    private final AggregateFunction windowFunction;
    private final List<Window> windows;
    private long allowedLateness;
    private long watermarkEvictionPeriod = 1000;

    //Alowed latency is in miliseconds
    public KeyedScottyWindowOperator(AggregateFunction windowFunction, long allowedLateness) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
        this.allowedLateness = allowedLateness;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperatorMap = new HashMap<>();
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "sum"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //The input is a tuple
        Key currentKey = (Key) tuple.getValue(0);
        if (!slicingWindowOperatorMap.containsKey(currentKey)) {
            slicingWindowOperatorMap.put(currentKey, initWindowOperator());
        }
        SlicingWindowOperator<Value> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
        //We only process the Value of a tuple
        slicingWindowOperator.processElement((Value) tuple.getValue(1), tuple.getLong(2));
        processWatermark(tuple.getLong(2), basicOutputCollector);
    }

    private void processWatermark(long timeStamp, BasicOutputCollector basicOutputCollector) {
        // Every tuple represents a Watermark with its timestamp.
        // A watermark is processed if it is greater than the old watermark, i.e. monotonically increasing.
        // We process watermarks every watermarkEvictionPeriod in event-time
        if (timeStamp > lastWatermark + watermarkEvictionPeriod) {
            for (Map.Entry<Key, SlicingWindowOperator<Value>> keyOperator : this.slicingWindowOperatorMap.entrySet()) {
                List<AggregateWindow> aggregates = keyOperator.getValue().processWatermark(timeStamp);
                for (AggregateWindow<Value> aggregateWindow : aggregates) {
                    // emit the window with the key it belongs to, not the key of the current tuple
                    if (aggregateWindow.hasValue())
                        basicOutputCollector.emit(new Values(keyOperator.getKey(), aggregateWindow));
                }
            }
            lastWatermark = timeStamp;
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
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

    /**
     * Register a new @{@link Window} definition to the ActiveWindow Operator.
     * For example {@link SlidingWindow} or {@link TumblingWindow}
     *
     * @param window the new window definition
     */
    public void addWindow(Window window) {
        windows.add(window);
    }

}
