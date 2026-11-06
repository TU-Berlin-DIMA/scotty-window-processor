package stream.scotty.slicing.aggregationstore;

public interface AggregationStoreFactory {

    <InputType> AggregationStore<InputType> createAggregationStore();
}
