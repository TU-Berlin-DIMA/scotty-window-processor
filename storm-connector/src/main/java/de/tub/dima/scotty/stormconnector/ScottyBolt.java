package de.tub.dima.scotty.stormconnector;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.state.memory.*;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/*
 * @author Batuhan Tüter
 * Scotty Window Operation connector for Apache Storm
 * It is implemented as a BasicBolt, thay can be used in any Storm Topology
 *
 * Input tuples must be in the following format:
 * <"tuple",key,value,timeStamp>

 * Input watermarks must be in the following format:
 * <"",key,value(not used),timeStamp>

 * */

public class ScottyBolt<Key, Value> extends BaseBasicBolt {

    private MemoryStateFactory stateFactory;
    //Key and Operator
    private HashMap<Key, SlicingWindowOperator<Value>> slicingWindowOperatorMap;
    private long lastWatermark;

    //Input,?,FinalAggregateType
    private final AggregateFunction<Value, ?, Value> windowFunction;
    private final List<Window> windows;

    public ScottyBolt(AggregateFunction<Value, ?, Value> windowFunction) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        this.stateFactory = new MemoryStateFactory();
        slicingWindowOperatorMap = new HashMap<>();
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "sum"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //The input is a tuple
        if (tuple.getString(0).equals("tuple")) {
            Object currentKey = tuple.getValue(1);
            if (!slicingWindowOperatorMap.containsKey(currentKey)) {
                slicingWindowOperatorMap.put((Key) currentKey, initWindowOperator());
            }
            SlicingWindowOperator<Value> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
            //We only process the Value of a tuple
            slicingWindowOperator.processElement((Value) tuple.getValue(2), tuple.getLong(3));
        }
        //Input is a watermark
        else
            processWatermark((Key) tuple.getValue(1), tuple.getLong(3), basicOutputCollector);
    }

    private void processWatermark(Key currentKey, long timeStamp, BasicOutputCollector basicOutputCollector) {
        //We use the timestamp field that is added to the watermark tuples
        long currentWaterMark = timeStamp;

        if (currentWaterMark > this.lastWatermark) {
            SlicingWindowOperator<Value> slicingWindowOperator = slicingWindowOperatorMap.get(currentKey);
            if (slicingWindowOperator != null) {
                List<AggregateWindow> aggregates = slicingWindowOperator.processWatermark(currentWaterMark);
                for (AggregateWindow<Value> aggregateWindow : aggregates) {
                    basicOutputCollector.emit(new Values(currentKey, aggregateWindow));
                }
                this.lastWatermark = currentWaterMark;
            }

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
