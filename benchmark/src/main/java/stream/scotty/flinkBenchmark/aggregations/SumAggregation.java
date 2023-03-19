package stream.scotty.flinkBenchmark.aggregations;

import stream.scotty.core.windowFunction.*;
import org.apache.flink.api.java.tuple.*;

import java.io.*;

public class SumAggregation implements InvertibleReduceAggregateFunction<Tuple4<String, Integer, Long, Long>>, Serializable {

    @Override
    public Tuple4<String, Integer, Long, Long> invert(Tuple4<String, Integer, Long, Long> currentAggregate, Tuple4<String, Integer, Long, Long> toRemove) {
        return new Tuple4<>(currentAggregate.f0, currentAggregate.f1 - toRemove.f1, currentAggregate.f2, currentAggregate.f3);
    }

    @Override
    public Tuple4<String, Integer, Long, Long> combine(Tuple4<String, Integer, Long, Long> partialAggregate1, Tuple4<String, Integer, Long, Long> partialAggregate2) {
        return new Tuple4<>(partialAggregate1.f0, partialAggregate1.f1 + partialAggregate2.f1, partialAggregate1.f2, partialAggregate1.f3);
    }
}