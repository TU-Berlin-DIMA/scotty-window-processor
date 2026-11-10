package stream.scotty.core.windowFunction.builtin;

import stream.scotty.core.windowFunction.CommutativeAggregateFunction;
import stream.scotty.core.windowFunction.InvertibleAggregateFunction;

/**
 * Number of tuples in a window.
 * @param <InputType> type of the input tuples, can be anything
 */
public class Count<InputType> implements
        InvertibleAggregateFunction<InputType, Long, Long>,
        CommutativeAggregateFunction<InputType, Long, Long> {

    @Override
    public Long lift(InputType inputTuple) {
        return 1L;
    }

    @Override
    public Long combine(Long partialAggregate1, Long partialAggregate2) {
        return partialAggregate1 + partialAggregate2;
    }

    @Override
    public Long invert(Long currentAggregate, Long toRemove) {
        return currentAggregate - toRemove;
    }

    @Override
    public Long lower(Long aggregate) {
        return aggregate;
    }
}
