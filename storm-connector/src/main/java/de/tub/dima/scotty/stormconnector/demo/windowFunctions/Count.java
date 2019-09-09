package de.tub.dima.scotty.stormconnector.demo.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.InvertibleReduceAggregateFunction;

public class Count implements InvertibleReduceAggregateFunction<Integer> {

    @Override
    public Integer lift(Integer inputTuple) {
        return 1;
    }

    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        return partialAggregate1+partialAggregate2;
    }

    @Override
    public Integer invert(Integer currentAggregate, Integer toRemove) {
        return currentAggregate-1;
    }
}
