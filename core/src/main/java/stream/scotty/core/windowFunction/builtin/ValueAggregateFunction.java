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

import stream.scotty.core.windowFunction.AggregateFunction;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Base class of the built-in functions that aggregate a value extracted from each input tuple.
 * <p>
 * The extractor is transient and written by Java serialization only.
 * Connectors ship aggregate functions with Java serialization, which supports serializable lambdas.
 * Field-based serializers like Kryo cannot serialize lambdas. Flink uses Kryo to copy the emitted
 * window results, which reference the aggregate function, but only need it to lower partial aggregates.
 */
abstract class ValueAggregateFunction<InputType, ValueType, PartialAggregateType, FinalAggregateType>
        implements AggregateFunction<InputType, PartialAggregateType, FinalAggregateType> {

    private transient ValueExtractor<InputType, ? extends ValueType> valueExtractor;

    ValueAggregateFunction(ValueExtractor<InputType, ? extends ValueType> valueExtractor) {
        this.valueExtractor = valueExtractor;
    }

    ValueType extract(InputType inputTuple) {
        return valueExtractor.apply(inputTuple);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(valueExtractor);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        valueExtractor = (ValueExtractor<InputType, ? extends ValueType>) in.readObject();
    }
}
