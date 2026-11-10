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
package stream.scotty.microbenchmark;

import stream.scotty.slicing.state.AggregateState;
import stream.scotty.state.memory.MemoryStateFactory;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collections;

@State(Scope.Benchmark)
public class AggregationStoreBenchmark {


    private long ts;
    private long n;
    private AggregateState aggregationState;
    private AggregationStateInline aggregationStateInline;

    @Setup(Level.Iteration)
    public void setupIteration() throws Exception {
        n = 0;
        ts = 0;
        MemoryStateFactory memoryStateFactory = new MemoryStateFactory();
        ReduceAggregateFunction<Integer> wf = new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
                return partialAggregate1 + partialAggregate2;
            }
        };
        this.aggregationState = new AggregateState(memoryStateFactory, Collections.singletonList(wf));
        this.aggregationStateInline  = new AggregationStateInline(memoryStateFactory);
    }



    @Benchmark()
    public void benchmarkInlineAggStore() throws Exception {
        aggregationStateInline.addElement(10);
    }


    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(AggregationStoreBenchmark.class.getName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }


}
