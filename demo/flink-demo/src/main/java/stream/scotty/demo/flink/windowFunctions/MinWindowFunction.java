package stream.scotty.demo.flink.windowFunctions;

import stream.scotty.core.windowFunction.*;
import org.apache.flink.api.java.tuple.*;

import java.io.*;

public class MinWindowFunction implements ReduceAggregateFunction<Tuple2<Integer, Integer>>, Serializable {

    @Override
    public Tuple2<Integer, Integer> combine(Tuple2<Integer, Integer> partialAggregate1, Tuple2<Integer, Integer> partialAggregate2) {
        return new Tuple2<>(partialAggregate1.f0, Math.min(partialAggregate1.f1, partialAggregate2.f1));
    }
}