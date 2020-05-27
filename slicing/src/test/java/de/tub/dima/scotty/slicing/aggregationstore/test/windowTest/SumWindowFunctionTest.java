package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.windowFunction.InvertibleReduceAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class SumWindowFunctionTest implements InvertibleReduceAggregateFunction<Tuple2<Integer, Integer>>, Serializable{

    @Override
    public Tuple2<Integer, Integer> invert(Tuple2<Integer, Integer> currentAggregate, Tuple2<Integer, Integer> toRemove) {
        return new Tuple2<>(currentAggregate.f0, currentAggregate.f1 - toRemove.f1);
    }

    @Override
    public Tuple2<Integer, Integer> combine(Tuple2<Integer, Integer> partialAggregate1, Tuple2<Integer, Integer> partialAggregate2) {
        return new Tuple2<>(partialAggregate2.f0, partialAggregate1.f1 + partialAggregate2.f1);
    }
}