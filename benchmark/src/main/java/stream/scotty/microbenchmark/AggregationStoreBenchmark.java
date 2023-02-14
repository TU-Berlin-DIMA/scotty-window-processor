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
