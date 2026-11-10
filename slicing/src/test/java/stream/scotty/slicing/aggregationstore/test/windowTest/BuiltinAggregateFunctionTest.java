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
package stream.scotty.slicing.aggregationstore.test.windowTest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.builtin.*;
import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;

import java.util.List;

/**
 * Runs the built-in aggregate functions of the core module through the slicing window operator.
 */
public class BuiltinAggregateFunctionTest {

    @Test
    public void outOfOrderCountWindow() {
        // out-of-order tuples in count-based windows shift tuples between slices,
        // which uses invert for Sum, Count and Mean and recomputes Min and Max
        SlicingWindowOperator<Integer> slicingWindowOperator = new SlicingWindowOperator<>(new MemoryStateFactory());
        slicingWindowOperator.addWindowFunction(Sum.<Integer>create());
        slicingWindowOperator.addWindowFunction(new Count<>());
        slicingWindowOperator.addWindowFunction(Mean.<Integer>create());
        slicingWindowOperator.addWindowFunction(Min.<Integer>create());
        slicingWindowOperator.addWindowFunction(Max.<Integer>create());
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 3));

        slicingWindowOperator.processElement(5, 1);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(7, 29);
        slicingWindowOperator.processElement(3, 39);
        // out of order
        slicingWindowOperator.processElement(2, 10);
        slicingWindowOperator.processElement(9, 50);
        slicingWindowOperator.processElement(4, 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(55);

        // first window: 5, 2, 1
        List<Object> first = resultWindows.get(0).getAggValues();
        Assert.assertEquals(8, first.get(0));
        Assert.assertEquals(3L, first.get(1));
        Assert.assertEquals(8.0 / 3, (Double) first.get(2), 0.0);
        Assert.assertEquals(1, first.get(3));
        Assert.assertEquals(5, first.get(4));

        // second window: 7, 3, 9
        List<Object> second = resultWindows.get(1).getAggValues();
        Assert.assertEquals(19, second.get(0));
        Assert.assertEquals(3L, second.get(1));
        Assert.assertEquals(19.0 / 3, (Double) second.get(2), 0.0);
        Assert.assertEquals(3, second.get(3));
        Assert.assertEquals(9, second.get(4));
    }

    @Test
    public void slidingWindowWithExtractedValues() {
        SlicingWindowOperator<Tuple2<Integer, Integer>> slicingWindowOperator = new SlicingWindowOperator<>(new MemoryStateFactory());
        slicingWindowOperator.addWindowFunction(new Sum<>((Tuple2<Integer, Integer> tuple) -> tuple.f1));
        slicingWindowOperator.addWindowFunction(new Mean<>((Tuple2<Integer, Integer> tuple) -> tuple.f1));
        slicingWindowOperator.addWindowFunction(new Max<>((Tuple2<Integer, Integer> tuple) -> tuple.f1));
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 5));

        slicingWindowOperator.processElement(new Tuple2<>(1, 1), 1);
        slicingWindowOperator.processElement(new Tuple2<>(1, 2), 6);
        slicingWindowOperator.processElement(new Tuple2<>(1, 4), 12);
        // out of order
        slicingWindowOperator.processElement(new Tuple2<>(1, 8), 3);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);

        // window [0, 10): 1, 8, 2
        List<Object> first = windowStartingAt(resultWindows, 0).getAggValues();
        Assert.assertEquals(11, first.get(0));
        Assert.assertEquals(11.0 / 3, (Double) first.get(1), 0.0);
        Assert.assertEquals(8, first.get(2));

        // window [5, 15): 2, 4
        List<Object> second = windowStartingAt(resultWindows, 5).getAggValues();
        Assert.assertEquals(6, second.get(0));
        Assert.assertEquals(3.0, (Double) second.get(1), 0.0);
        Assert.assertEquals(4, second.get(2));
    }

    private static AggregateWindow windowStartingAt(List<AggregateWindow> windows, long start) {
        for (AggregateWindow window : windows) {
            if (window.getStart() == start) {
                return window;
            }
        }
        throw new AssertionError("No window starting at " + start + " in " + windows);
    }
}
