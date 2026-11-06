package stream.scotty.demo.beam.windowFunctions;

import stream.scotty.core.windowFunction.AggregateFunction;
import stream.scotty.core.windowFunction.CloneablePartialStateFunction;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class Quantile implements AggregateFunction<
        KV<Integer, Integer>,
        KV<Integer, QuantileTreeMap>,
        KV<Integer,Integer>>,
        CloneablePartialStateFunction<KV<Integer,QuantileTreeMap>>, Serializable {
    private final double quantile;

    public Quantile(double quantile) {
        this.quantile = quantile;
    }

    @Override
    public KV<Integer, Integer> lower(KV<Integer, QuantileTreeMap> aggregate) {
        return KV.of(aggregate.getKey(),aggregate.getValue().getQuantile());
    }

    @Override
    public KV<Integer, QuantileTreeMap> lift(KV<Integer, Integer> inputTuple) {
        return KV.of(inputTuple.getKey(),new QuantileTreeMap(Math.toIntExact(inputTuple.getValue()),quantile));
    }

    @Override
    public KV<Integer, QuantileTreeMap> combine(KV<Integer, QuantileTreeMap> partialAggregate1, KV<Integer, QuantileTreeMap> partialAggregate2) {
        return KV.of(partialAggregate1.getKey(),partialAggregate1.getValue().merge(partialAggregate2.getValue()));
    }

    @Override
    public KV<Integer, QuantileTreeMap> liftAndCombine(KV<Integer, QuantileTreeMap> partialAggregate, KV<Integer, Integer> inputTuple) {
        partialAggregate.getValue().addValue(Math.toIntExact(inputTuple.getValue()));
        return partialAggregate;
    }

    @Override
    public KV<Integer, QuantileTreeMap> clone(KV<Integer, QuantileTreeMap> partialAggregate) {
        return KV.of(partialAggregate.getKey(),partialAggregate.getValue().clone());
    }
}