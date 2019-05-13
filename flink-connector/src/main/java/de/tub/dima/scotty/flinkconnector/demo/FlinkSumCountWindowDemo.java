package de.tub.dima.scotty.flinkconnector.demo;

import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.flinkconnector.*;
import de.tub.dima.scotty.flinkconnector.demo.windowFunctions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;

import java.io.*;

public class FlinkSumCountWindowDemo implements Serializable {


    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment sev = StreamExecutionEnvironment.createLocalEnvironment();
        sev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sev.setParallelism(1);
        sev.setMaxParallelism(1);

        DataStream<Tuple2<Integer, Integer>> stream = sev.addSource(new DemoSource());

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> processingFunction =
                new KeyedScottyWindowOperator<>(new SumWindowFunction());

        processingFunction.addWindow(new TumblingWindow(WindowMeasure.Count, 1000));

        stream
                .keyBy(0)
                .process(processingFunction)
                .print();

        sev.execute("demo");
    }

}
