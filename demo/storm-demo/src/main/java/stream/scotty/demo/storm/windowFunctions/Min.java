package stream.scotty.demo.storm.windowFunctions;

import stream.scotty.core.windowFunction.ReduceAggregateFunction;

public class Min implements ReduceAggregateFunction<Integer> {

    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        return Math.min(partialAggregate1,partialAggregate2);
    }
}
