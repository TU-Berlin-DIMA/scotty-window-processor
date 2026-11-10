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

import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.AggregateFunction;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import stream.scotty.core.windowType.PunctuationWindow;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PunctuationWindowTupleTest {

    /*
     * The stream mixes data tuples (Integer, Integer) and punctuation tuples (String, Integer), so the
     * first field is typed as Object. Punctuation tuples are part of the stream and are aggregated as well
     * (with value 0). The sum function must not cast the first field to Integer: javac 9+ inserts such a
     * cast for Tuple2<Integer, Integer>.f0, which made this test fail on JDK 9+ (see issue #52).
     */
    private static class SumWindowFunction implements ReduceAggregateFunction<Tuple2<Object, Integer>> {
        @Override
        public Tuple2<Object, Integer> combine(Tuple2<Object, Integer> partialAggregate1, Tuple2<Object, Integer> partialAggregate2) {
            return new Tuple2<>(partialAggregate2.f0, partialAggregate1.f1 + partialAggregate2.f1);
        }
    }

    private SlicingWindowOperator<Tuple2<Object, Integer>> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<>(stateFactory);
    }
    @Test
    public void inOrderTest() {
        final AggregateFunction<Tuple2<Object, Integer>, ?, Tuple2<Object, Integer>> windowFunction = new SumWindowFunction();
        slicingWindowOperator.addWindowFunction(windowFunction);

        Tuple2<Object, Object> punctuation = new Tuple2<>("key", ".*"); //string matching
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(punctuation));

        slicingWindowOperator.processElement(new Tuple2<>(1,1), 1);
        slicingWindowOperator.processElement(new Tuple2<>("key",0), 10);
        slicingWindowOperator.processElement(new Tuple2<>(1,2), 19);
        slicingWindowOperator.processElement(new Tuple2<>(1,3), 23);
        slicingWindowOperator.processElement(new Tuple2<>("key",0), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,4), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,5), 49);
        slicingWindowOperator.processElement(new Tuple2<>("key",0), 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,new Tuple2<>(1,1));
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,new Tuple2<>(1,5));

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,new Tuple2<>(1,9));
    }

    @Test
    public void inOrderTestRegex() {
        final AggregateFunction<Tuple2<Object, Integer>, ?, Tuple2<Object, Integer>> windowFunction = new SumWindowFunction();
        slicingWindowOperator.addWindowFunction(windowFunction);

        Tuple2<Object, Object> punctuation = new Tuple2<>(".*", 0);
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(punctuation));

        slicingWindowOperator.processElement(new Tuple2<>(1,1), 1);
        slicingWindowOperator.processElement(new Tuple2<>("123",0), 10);
        slicingWindowOperator.processElement(new Tuple2<>(1,2), 19);
        slicingWindowOperator.processElement(new Tuple2<>(1,3), 23);
        slicingWindowOperator.processElement(new Tuple2<>("234",0), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,4), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,5), 49);
        slicingWindowOperator.processElement(new Tuple2<>("345",0), 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,new Tuple2<>(1,1));
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,new Tuple2<>(1,5));

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,new Tuple2<>(1,9));
    }

    @Test
    public void outOfOrderTupleTest() {
        final AggregateFunction<Tuple2<Object, Integer>, ?, Tuple2<Object, Integer>> windowFunction = new SumWindowFunction();
        slicingWindowOperator.addWindowFunction(windowFunction);

        Tuple2<Object, Integer> p = new Tuple2<>(1, 0);
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));

        Tuple2<Object, Integer> t = new Tuple2<>(1, 0);
        slicingWindowOperator.processElement(new Tuple2<>(1,1), 1);
        slicingWindowOperator.processElement(t, 10);
        slicingWindowOperator.processElement(new Tuple2<>(1,2), 19);
        slicingWindowOperator.processElement(t, 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,3), 23); //out-of-order tuple
        slicingWindowOperator.processElement(new Tuple2<>(1,5), 49);
        slicingWindowOperator.processElement(t, 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,new Tuple2<>(1,1));
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,new Tuple2<>(1,5));

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,new Tuple2<>(1,5));
    }

    @Test
    public void twoWindowsTest() {
        final AggregateFunction<Tuple2<Object, Integer>, ?, Tuple2<Object, Integer>> windowFunction = new SumWindowFunction();
        slicingWindowOperator.addWindowFunction(windowFunction);

        Tuple2<Object, Integer> p = new Tuple2<>(1, 0);
        Tuple2<Object, Integer> p2 = new Tuple2<>("123", 0);
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p2));

        Tuple2<Object, Integer> t = new Tuple2<>(1, 0);
        slicingWindowOperator.processElement(new Tuple2<>(1,1), 1);
        slicingWindowOperator.processElement(t, 10);
        slicingWindowOperator.processElement(new Tuple2<>(1,2), 19);
        slicingWindowOperator.processElement(new Tuple2<>("123", 0), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,3), 23); //out-of-order tuple
        slicingWindowOperator.processElement(new Tuple2<>(1,5), 49);
        slicingWindowOperator.processElement(t, 51);
        slicingWindowOperator.processElement(new Tuple2<>("123", 0), 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,new Tuple2<>(1,1));
        WindowAssert.assertEquals(resultWindows.get(1), 1,30,new Tuple2<>(1,6));

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 10,51,new Tuple2<>(1,10));
        WindowAssert.assertEquals(resultWindows.get(1), 30,51,new Tuple2<>(1,5));
    }

}
