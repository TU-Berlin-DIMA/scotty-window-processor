package stream.scotty.demo.flink.windowFunctions;

import stream.scotty.core.windowFunction.*;
import org.apache.flink.api.java.tuple.*;

import java.io.*;

public class SumWindowFunction implements InvertibleReduceAggregateFunction<Tuple2<Integer, Integer>>, Serializable {

    @Override
    public Tuple2<Integer, Integer> invert(Tuple2<Integer, Integer> currentAggregate, Tuple2<Integer, Integer> toRemove) {
        return new Tuple2<>(currentAggregate.f0, currentAggregate.f1 - toRemove.f1);
    }

    @Override
    public Tuple2<Integer, Integer> combine(Tuple2<Integer, Integer> partialAggregate1, Tuple2<Integer, Integer> partialAggregate2) {
        return new Tuple2<>(partialAggregate1.f0, partialAggregate1.f1 + partialAggregate2.f1);
    }
}