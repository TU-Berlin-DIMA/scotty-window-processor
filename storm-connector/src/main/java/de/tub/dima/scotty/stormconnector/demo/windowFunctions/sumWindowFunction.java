package de.tub.dima.scotty.stormconnector.demo.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.InvertibleReduceAggregateFunction;

public class sumWindowFunction implements InvertibleReduceAggregateFunction<Integer> {
    @Override
    public Integer invert(Integer currentAggregate, Integer toRemove) {
        return currentAggregate-toRemove;
    }

    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        return partialAggregate1+partialAggregate2;
    }
}
