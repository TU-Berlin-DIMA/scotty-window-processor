package de.tub.dima.scotty.flinkconnector.demo;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.flinkconnector.*;
import de.tub.dima.scotty.flinkconnector.demo.windowFunctions.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;

import java.io.*;

public class FlinkQuantileDemo implements Serializable {

    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment sev = StreamExecutionEnvironment.createLocalEnvironment();
        sev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<Integer, Integer>> stream = sev.addSource(new DemoSource());

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> windowOperator =
                new KeyedScottyWindowOperator<>(new QuantileWindowFunction(0.5));

        windowOperator.addWindow(new TumblingWindow(WindowMeasure.Time, 1000));

        stream
                .keyBy(0)
                .process(windowOperator)
                .flatMap((FlatMapFunction<AggregateWindow<Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>) (tuple2AggregateWindow, collector) -> {
                        for(Tuple2<Integer,Integer> value: tuple2AggregateWindow.getAggValue()){
                            collector.collect(value);
                        }
                })
                .print();

        sev.execute("demo");
    }

}
