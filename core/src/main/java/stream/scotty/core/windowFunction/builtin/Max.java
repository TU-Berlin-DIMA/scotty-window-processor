/*
 * Copyright The Scotty Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stream.scotty.core.windowFunction.builtin;

import stream.scotty.core.windowFunction.CommutativeAggregateFunction;

/**
 * Maximum of the values in a window.
 * Works for all {@link Comparable} values, e.g. numbers or strings.
 * Max is not invertible, Scotty recomputes the aggregate if a tuple has to be removed from a slice.
 * <p>
 * Use {@link #create()} if the input tuples are the values,
 * or {@link #Max(ValueExtractor)} to use a value extracted from each input tuple.
 * @param <InputType> type of the input tuples
 * @param <ValueType> type of the compared values
 */
public class Max<InputType, ValueType extends Comparable<? super ValueType>>
        extends ValueAggregateFunction<InputType, ValueType, ValueType, ValueType> implements
        CommutativeAggregateFunction<InputType, ValueType, ValueType> {

    public Max(ValueExtractor<InputType, ValueType> valueExtractor) {
        super(valueExtractor);
    }

    /**
     * @return a maximum over input tuples that are comparable values themselves.
     */
    public static <ValueType extends Comparable<? super ValueType>> Max<ValueType, ValueType> create() {
        return new Max<>(value -> value);
    }

    @Override
    public ValueType lift(InputType inputTuple) {
        return extract(inputTuple);
    }

    @Override
    public ValueType combine(ValueType partialAggregate1, ValueType partialAggregate2) {
        return partialAggregate1.compareTo(partialAggregate2) >= 0 ? partialAggregate1 : partialAggregate2;
    }

    @Override
    public ValueType lower(ValueType aggregate) {
        return aggregate;
    }
}
