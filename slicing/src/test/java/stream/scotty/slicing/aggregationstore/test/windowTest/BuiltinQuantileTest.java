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
import stream.scotty.core.windowFunction.builtin.Quantile;
import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Compares the built-in Quantile function in the slicing window operator with a naive quantile over random streams.
 */
public class BuiltinQuantileTest {

    private static final int[] PERCENTS = {1, 25, 50, 90, 100};

    /**
     * Nearest-rank quantile: the value at rank ceil(percent / 100 * n) of the sorted values.
     */
    private static Integer naiveQuantile(List<Integer> values, int percent) {
        List<Integer> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int rank = (percent * sorted.size() + 99) / 100;
        return sorted.get(rank - 1);
    }

    private static void assertQuantiles(List<Integer> values, AggregateWindow window) {
        List<Object> results = window.getAggValues();
        if (values.isEmpty()) {
            Assert.assertTrue("Window " + window + " has no values", results.isEmpty());
            return;
        }
        Assert.assertEquals(PERCENTS.length, results.size());
        for (int i = 0; i < PERCENTS.length; i++) {
            Assert.assertEquals("Quantile " + PERCENTS[i] + "% of " + window,
                    naiveQuantile(values, PERCENTS[i]), results.get(i));
        }
    }

    @Test
    public void slidingTimeWindows() {
        // each sliding window combines the partial aggregates of five slices, which other windows share
        SlicingWindowOperator<Tuple2<Integer, Integer>> slicingWindowOperator = new SlicingWindowOperator<>(new MemoryStateFactory());
        for (int percent : PERCENTS) {
            slicingWindowOperator.addWindowFunction(new Quantile<>(percent / 100.0, (Tuple2<Integer, Integer> tuple) -> tuple.f1));
        }
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 100, 20));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 50));

        Random random = new Random(42);
        int delay = 30;
        List<Long> timestamps = new ArrayList<>();
        List<Integer> values = new ArrayList<>();
        List<AggregateWindow> resultWindows = new ArrayList<>();
        long maxTs = 0;
        for (int i = 0; i < 5000; i++) {
            long ts;
            if (maxTs > delay && random.nextInt(10) == 0) {
                // out of order, but after the last watermark
                ts = maxTs - random.nextInt(delay);
            } else {
                ts = maxTs + random.nextInt(4);
                maxTs = ts;
            }
            // few distinct values, so that values occur multiple times in a slice and across slices
            int value = random.nextInt(30);
            timestamps.add(ts);
            values.add(value);
            slicingWindowOperator.processElement(new Tuple2<>(1, value), ts);
            if (i % 100 == 99) {
                resultWindows.addAll(slicingWindowOperator.processWatermark(maxTs - delay));
            }
        }
        resultWindows.addAll(slicingWindowOperator.processWatermark(maxTs + 100));

        int windowsWithValues = 0;
        for (AggregateWindow window : resultWindows) {
            List<Integer> windowValues = new ArrayList<>();
            for (int i = 0; i < timestamps.size(); i++) {
                if (window.getStart() <= timestamps.get(i) && timestamps.get(i) < window.getEnd()) {
                    windowValues.add(values.get(i));
                }
            }
            assertQuantiles(windowValues, window);
            if (!windowValues.isEmpty()) {
                windowsWithValues++;
            }
        }
        // all windows up to the last tuple: maxTs / 20 sliding and maxTs / 50 tumbling windows
        Assert.assertTrue("Only " + windowsWithValues + " windows", windowsWithValues >= maxTs / 20 + maxTs / 50);
    }

    @Test
    public void outOfOrderCountWindows() {
        // out-of-order tuples in count-based windows shift the last tuple of each later slice to the next slice,
        // which removes it from the slice's partial aggregate with invert
        SlicingWindowOperator<Integer> slicingWindowOperator = new SlicingWindowOperator<>(new MemoryStateFactory());
        for (int percent : PERCENTS) {
            slicingWindowOperator.addWindowFunction(Quantile.<Integer>create(percent / 100.0));
        }
        int windowSize = 7;
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, windowSize));

        Random random = new Random(7);
        int tupleCount = 700;
        // distinct timestamps, the first tuple has the smallest timestamp
        long[] timestamps = new long[tupleCount];
        for (int i = 0; i < tupleCount; i++) {
            timestamps[i] = 10L * i + 1;
        }
        for (int i = 2; i < tupleCount; i++) {
            if (random.nextInt(4) == 0) {
                int j = Math.max(1, i - 1 - random.nextInt(2 * windowSize));
                long swap = timestamps[i];
                timestamps[i] = timestamps[j];
                timestamps[j] = swap;
            }
        }
        int[] values = new int[tupleCount];
        for (int i = 0; i < tupleCount; i++) {
            values[i] = random.nextInt(10);
            slicingWindowOperator.processElement(values[i], timestamps[i]);
        }
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(10L * tupleCount + 100);

        // count windows contain the tuples in timestamp order
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < tupleCount; i++) {
            order.add(i);
        }
        order.sort((a, b) -> Long.compare(timestamps[a], timestamps[b]));

        for (AggregateWindow window : resultWindows) {
            List<Integer> windowValues = new ArrayList<>();
            for (long position = window.getStart(); position < window.getEnd() && position < tupleCount; position++) {
                windowValues.add(values[order.get((int) position)]);
            }
            assertQuantiles(windowValues, window);
        }
        Assert.assertTrue("Only " + resultWindows.size() + " windows", resultWindows.size() >= tupleCount / windowSize - 1);
    }
}
