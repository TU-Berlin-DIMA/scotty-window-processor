package de.tub.dima.scotty.core.windowFunction;

public interface CloneablePartialStateFunction<PartialAggregateType> {

    /**
     * Method that returns a deep copy of the partial state.
     * @param partialAggregate original object
     * @return cloned object
     */
    public PartialAggregateType clone(PartialAggregateType partialAggregate);

}
