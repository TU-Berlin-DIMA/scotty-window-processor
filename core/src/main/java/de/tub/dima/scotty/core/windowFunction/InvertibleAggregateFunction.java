package de.tub.dima.scotty.core.windowFunction;

public interface InvertibleAggregateFunction<InputType, PartialAggregateType, FinalAggregateType> extends AggregateFunction<InputType, PartialAggregateType, FinalAggregateType> {
    /**
     * Removes one partial aggregate from another with an incremental operation.
     * @param currentAggregate
     * @param toRemove
     * @return
     */
    PartialAggregateType invert(PartialAggregateType currentAggregate, PartialAggregateType toRemove);

    default PartialAggregateType liftAndInvert(PartialAggregateType partialAggregate, InputType toRemove){
        PartialAggregateType lifted = lift(toRemove);
        return invert(partialAggregate, lifted);
    };
}
