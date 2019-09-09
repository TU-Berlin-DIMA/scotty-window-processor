package de.tub.dima.scotty.stormconnector.demo.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;

public class Max implements ReduceAggregateFunction<Integer> {

    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        return Math.max(partialAggregate1,partialAggregate2);
    }
}
