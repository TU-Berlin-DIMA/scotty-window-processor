package de.tub.dima.scotty.slicing.aggregationstore;

public interface AggregationStoreFactory {

    <InputType> AggregationStore<InputType> createAggregationStore();
}
