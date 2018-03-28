package de.tub.dima.scotty.microbenchmark;

import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class SlicingWindowOperatorBenchmark {

    private SlicingWindowOperator<Integer> windowOperator;

    private long ts;
    private long n;

    @Setup(Level.Iteration)
    public void setupIteration() throws Exception {
        n = 0;
        ts = 0;
        MemoryStateFactory memoryStateFactory = new MemoryStateFactory();
        this.windowOperator = new SlicingWindowOperator<>(memoryStateFactory);
        this.windowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        this.windowOperator.addWindowFunction(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
                return partialAggregate1 + partialAggregate2;
            }
        });
    }

    @Benchmark()
    public void benchmarkSameSlice() throws Exception {
        windowOperator.processElement(10, 0);
    }

    @Benchmark()
    public void benchmarkSameSlice1000() throws Exception {

        n++;
        if (n == 1000) {
            n = 0;
            ts += 10;
        }

        windowOperator.processElement(10, ts);
    }


    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(SlicingWindowOperatorBenchmark.class.getName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }


}
