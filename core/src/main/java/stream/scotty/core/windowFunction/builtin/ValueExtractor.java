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
