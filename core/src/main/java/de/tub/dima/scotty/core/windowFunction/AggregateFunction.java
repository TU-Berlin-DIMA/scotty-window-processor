package de.tub.dima.scotty.core.windowFunction;


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
     * @param partialAggregate1
     * @param partialAggregate2
     * @return combined PartialAggregate
     */
    PartialAggregateType combine(PartialAggregateType partialAggregate1, PartialAggregateType partialAggregate2);

    /**
     * Transforms a partial aggregate to a final aggregate.
     * In our example, the lower function computes the average from sum and count:
     * ⟨sum, count⟩ 7→ sum/count.
     * @param aggregate
     * @return final Aggregate
     */
    FinalAggregateType lower(PartialAggregateType aggregate);

}
