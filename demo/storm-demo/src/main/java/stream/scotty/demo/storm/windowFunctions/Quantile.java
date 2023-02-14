package stream.scotty.demo.storm.windowFunctions;

import stream.scotty.core.windowFunction.AggregateFunction;
import stream.scotty.core.windowFunction.CloneablePartialStateFunction;

public class Quantile implements AggregateFunction<Integer, QuantileTreeMap, Integer>, CloneablePartialStateFunction<QuantileTreeMap> {
    private final double quantile;

    public Quantile(double quantile) {
        this.quantile = quantile;
    }

    @Override
    public QuantileTreeMap lift(Integer inputTuple) {
        return new QuantileTreeMap(inputTuple,quantile);
    }

    @Override
    public QuantileTreeMap combine(QuantileTreeMap partialAggregate1, QuantileTreeMap partialAggregate2) {
        return partialAggregate1.merge(partialAggregate2);
    }

    @Override
    public QuantileTreeMap liftAndCombine(QuantileTreeMap partialAggregate, Integer inputTuple) {
        partialAggregate.addValue(inputTuple);
        return partialAggregate;
    }

    @Override
    public Integer lower(QuantileTreeMap aggregate) {
        return aggregate.getQuantile();
    }

    @Override
    public QuantileTreeMap clone(QuantileTreeMap partialAggregate) {
        return partialAggregate.clone();
    }
}
