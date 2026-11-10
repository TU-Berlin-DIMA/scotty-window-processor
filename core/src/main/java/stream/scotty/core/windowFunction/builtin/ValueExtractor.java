package stream.scotty.core.windowFunction.builtin;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Extracts the value to aggregate from an input tuple, e.g. a field of a Tuple2 or the value of a KV.
 * The extractor is part of the aggregate function and thus has to be serializable.
 * Lambdas and method references assigned to this interface are serializable.
 * @param <InputType> type of the input tuples
 * @param <ValueType> type of the extracted value
 */
@FunctionalInterface
public interface ValueExtractor<InputType, ValueType> extends Function<InputType, ValueType>, Serializable {
}
