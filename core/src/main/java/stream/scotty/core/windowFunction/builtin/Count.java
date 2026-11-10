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

/**
 * Number of tuples in a window.
 * @param <InputType> type of the input tuples, can be anything
 */
public class Count<InputType> implements
        InvertibleAggregateFunction<InputType, Long, Long>,
        CommutativeAggregateFunction<InputType, Long, Long> {

    @Override
    public Long lift(InputType inputTuple) {
        return 1L;
    }

    @Override
    public Long combine(Long partialAggregate1, Long partialAggregate2) {
        return partialAggregate1 + partialAggregate2;
    }

    @Override
    public Long invert(Long currentAggregate, Long toRemove) {
        return currentAggregate - toRemove;
    }

    @Override
    public Long lower(Long aggregate) {
        return aggregate;
    }
}
