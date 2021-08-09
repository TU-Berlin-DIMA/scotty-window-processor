package de.tub.dima.scotty.demo.flink.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.*;
import org.apache.flink.api.java.tuple.*;

public class QuantileWindowFunction implements AggregateFunction<Tuple2<Integer, Integer>,
        Tuple2<Integer, QuantileTreeMap>,
        Tuple2<Integer, Integer>>,
        CloneablePartialStateFunction<Tuple2<Integer, QuantileTreeMap>> {

    private final double quantile;

    public QuantileWindowFunction(final double quantile) {
        this.quantile = quantile;
    }


    @Override
    public Tuple2<Integer, QuantileTreeMap> lift(Tuple2<Integer, Integer> inputTuple) {
        return new Tuple2<>(inputTuple.f0, new QuantileTreeMap(inputTuple.f1, quantile));
    }

    @Override
    public Tuple2<Integer, Integer> lower(Tuple2<Integer, QuantileTreeMap> aggregate) {
        return new Tuple2<>(aggregate.f0, aggregate.f1.getQuantile());
    }

    @Override
    public Tuple2<Integer, QuantileTreeMap> combine(Tuple2<Integer, QuantileTreeMap> partialAggregate1, Tuple2<Integer, QuantileTreeMap> partialAggregate2) {
        return new Tuple2<>(partialAggregate1.f0, partialAggregate1.f1.merge(partialAggregate2.f1));
    }

    @Override
    public Tuple2<Integer, QuantileTreeMap> liftAndCombine(Tuple2<Integer, QuantileTreeMap> partialAggregate, Tuple2<Integer, Integer> inputTuple) {
        partialAggregate.f1.addValue(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public Tuple2<Integer, QuantileTreeMap> clone(Tuple2<Integer, QuantileTreeMap> partialAggregate) {
        return new Tuple2<>(partialAggregate.f0, partialAggregate.f1.clone());
    }
}
