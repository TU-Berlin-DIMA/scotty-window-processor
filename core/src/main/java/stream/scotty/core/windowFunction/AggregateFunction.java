package stream.scotty.core.windowFunction;


import java.io.*;

public interface AggregateFunction<InputType, PartialAggregateType, FinalAggregateType> extends Serializable {

    /**
     * Transforms a tuple to a partial aggregate.
     * For example, consider an average computation. If a tuple ⟨p,v⟩
     * contains its position p and a value v, the lift function will transform
     * it to ⟨sum←v, count←1⟩, which is the partial aggregate of
     * that one tuple.
     * @param inputTuple input Tuple
     * @return PartialAggregate
     */
    PartialAggregateType lift(InputType inputTuple);

    /**
     * Computes the combined aggregate from partial aggregates.
     * Each incremental aggregation step results in one call of the combine function.
     * It is expected that this method returns new partial aggregate.
     * This method is used in two ways.
     * 1. To add a single element to the partial aggregate.
     * 2. To merge two big partial aggregates (or slices) , for example for emitting a final window.
     * For the second step it is needed to emit a completely new partial aggregate.
     * This can involve a deep copy of the partial aggregates.
     * To prevent this a AggregationFunction can implement {@link CloneablePartialStateFunction}.
     * In this case copy provides a cloned partial aggregate as the first argument.
     * @param partialAggregate1 the first original partial aggregate or a cloned object iff the AggregationFunction implements {@link CloneablePartialStateFunction}
     * @param partialAggregate2 the second ordinal partial aggregate.
     * @return combined PartialAggregate
     */
    PartialAggregateType combine(PartialAggregateType partialAggregate1, PartialAggregateType partialAggregate2);

    /**
     * Default implementation for combining the lift and combine function.
     * This is used by Scotty to add a single element to a slice.
     * For some AggregationFunctions it can be beneficial to implement a more efficient lift and combination function.
     * @param partialAggregate
     * @param inputTuple
     * @return
     */
    default PartialAggregateType liftAndCombine(PartialAggregateType partialAggregate, InputType inputTuple){
        PartialAggregateType lifted = lift(inputTuple);
        return combine(partialAggregate, lifted);
    };

    /**
     * Transforms a partial aggregate to a final aggregate.
     * In our example, the lower function computes the average from sum and count:
     * ⟨sum, count⟩ 7→ sum/count.
     * @param aggregate
     * @return final Aggregate
     */
    FinalAggregateType lower(PartialAggregateType aggregate);

}
