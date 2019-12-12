package de.tub.dima.scotty.samzaconnector.demo.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;

public class MaxWindowFunction implements ReduceAggregateFunction<Integer> {
    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        return Math.max(partialAggregate1,partialAggregate2);
    }
}
