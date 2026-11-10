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
import stream.scotty.core.windowFunction.InvertibleAggregateFunction;

import java.io.Serializable;

/**
 * Arithmetic mean of the values in a window as a Double.
 * The partial aggregate is the sum and the count of the values ({@link SumCount}).
 * <p>
 * Use {@link #create()} if the input tuples are the values,
 * or {@link #Mean(ValueExtractor)} to use a value extracted from each input tuple.
 * @param <InputType> type of the input tuples
 */
public class Mean<InputType>
        extends ValueAggregateFunction<InputType, Number, Mean.SumCount, Double> implements
        InvertibleAggregateFunction<InputType, Mean.SumCount, Double>,
        CommutativeAggregateFunction<InputType, Mean.SumCount, Double> {

    public Mean(ValueExtractor<InputType, ? extends Number> valueExtractor) {
        super(valueExtractor);
    }

    /**
     * @return a mean over input tuples that are numbers themselves.
     */
    public static <ValueType extends Number> Mean<ValueType> create() {
        return new Mean<>(value -> value);
    }

    @Override
    public SumCount lift(InputType inputTuple) {
        return new SumCount(extract(inputTuple).doubleValue(), 1);
    }

    @Override
    public SumCount combine(SumCount partialAggregate1, SumCount partialAggregate2) {
        return new SumCount(partialAggregate1.sum + partialAggregate2.sum, partialAggregate1.count + partialAggregate2.count);
    }

    @Override
    public SumCount invert(SumCount currentAggregate, SumCount toRemove) {
        return new SumCount(currentAggregate.sum - toRemove.sum, currentAggregate.count - toRemove.count);
    }

    @Override
    public Double lower(SumCount aggregate) {
        return aggregate.sum / aggregate.count;
    }

    /**
     * Immutable partial aggregate of {@link Mean}.
     */
    public static final class SumCount implements Serializable {

        private final double sum;
        private final long count;

        public SumCount(double sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public double getSum() {
            return sum;
        }

        public long getCount() {
            return count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SumCount sumCount = (SumCount) o;
            return Double.compare(sumCount.sum, sum) == 0 && count == sumCount.count;
        }

        @Override
        public int hashCode() {
            long temp = Double.doubleToLongBits(sum);
            int result = (int) (temp ^ (temp >>> 32));
            return 31 * result + (int) (count ^ (count >>> 32));
        }

        @Override
        public String toString() {
            return "SumCount{sum=" + sum + ", count=" + count + "}";
        }
    }
}
