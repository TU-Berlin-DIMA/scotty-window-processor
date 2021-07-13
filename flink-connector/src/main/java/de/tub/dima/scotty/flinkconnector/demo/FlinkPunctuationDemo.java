package de.tub.dima.scotty.flinkconnector.demo;

import de.tub.dima.scotty.core.windowType.PunctuationWindow;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import de.tub.dima.scotty.flinkconnector.demo.windowFunctions.SumWindowFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public class FlinkPunctuationDemo  implements Serializable  {
    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment sev = StreamExecutionEnvironment.createLocalEnvironment();
        sev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sev.setParallelism(1);
        sev.setMaxParallelism(1);

        DataStream<Tuple2<Integer, Integer>> stream = sev.addSource(new DemoSource());

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> processingFunction =
                new KeyedScottyWindowOperator<>(new SumWindowFunction());

        //define the punctuation, can have different values
        Tuple2 p = new Tuple2(1, 9); //every time a tuple with the value 9 is emitted, a new window starts

        processingFunction
                .addWindow(new PunctuationWindow(p));

        stream
                .keyBy(0)
                .process(processingFunction)
                .print();

        sev.execute("demo");
    }
}
