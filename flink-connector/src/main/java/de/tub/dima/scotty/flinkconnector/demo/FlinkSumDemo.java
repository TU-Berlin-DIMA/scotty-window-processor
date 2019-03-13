package de.tub.dima.scotty.flinkconnector.demo;

import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.flinkconnector.*;
import de.tub.dima.scotty.flinkconnector.demo.windowFunctions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;

import java.io.*;

public class FlinkSumDemo implements Serializable {


    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment sev = StreamExecutionEnvironment.createLocalEnvironment();
        sev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //sev.setBufferTimeout(10);
        sev.setParallelism(1);

        DataStream<Tuple2<Integer, Integer>> stream = sev.addSource(new DemoSource());

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> processingFunction =
                new KeyedScottyWindowOperator<>(new SumWindowFunction());

        processingFunction.addWindow(new TumblingWindow(WindowMeasure.Time, 2000));
        //processingFunction.addWindow(new SlidingWindow(WindowMeasure.Time, 1000,5000));

        stream
                .keyBy(0)
                .process(processingFunction)
                .print();

        sev.execute("demo");
    }

}
