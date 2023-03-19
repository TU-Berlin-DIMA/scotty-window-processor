package stream.scotty.core.windowFunction;

public interface InvertibleReduceAggregateFunction<Input> extends ReduceAggregateFunction<Input>, InvertibleAggregateFunction<Input, Input, Input> {


}
