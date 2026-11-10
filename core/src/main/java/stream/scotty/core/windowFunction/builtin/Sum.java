package stream.scotty.core.windowFunction.builtin;

import stream.scotty.core.windowFunction.CommutativeAggregateFunction;
import stream.scotty.core.windowFunction.InvertibleAggregateFunction;

/**
 * Sum of the values in a window.
 * The result has the type of the values, e.g. the sum of Integer values is an Integer.
 * Supported value types are Integer, Long, Float, Double, BigInteger and BigDecimal.
 * <p>
 * Use {@link #create()} if the input tuples are the values,
 * or {@link #Sum(ValueExtractor)} to sum a value extracted from each input tuple.
 * @param <InputType> type of the input tuples
 * @param <ValueType> type of the summed values
 */
public class Sum<InputType, ValueType extends Number>
        extends ValueAggregateFunction<InputType, ValueType, ValueType, ValueType> implements
        InvertibleAggregateFunction<InputType, ValueType, ValueType>,
        CommutativeAggregateFunction<InputType, ValueType, ValueType> {

    public Sum(ValueExtractor<InputType, ValueType> valueExtractor) {
        super(valueExtractor);
    }

    /**
     * @return a sum over input tuples that are numbers themselves.
     */
    public static <ValueType extends Number> Sum<ValueType, ValueType> create() {
        return new Sum<>(value -> value);
    }

    @Override
    public ValueType lift(InputType inputTuple) {
        return extract(inputTuple);
    }

    @Override
    public ValueType combine(ValueType partialAggregate1, ValueType partialAggregate2) {
        return Numbers.add(partialAggregate1, partialAggregate2);
    }

    @Override
    public ValueType invert(ValueType currentAggregate, ValueType toRemove) {
        return Numbers.subtract(currentAggregate, toRemove);
    }

    @Override
    public ValueType lower(ValueType aggregate) {
        return aggregate;
    }
}
