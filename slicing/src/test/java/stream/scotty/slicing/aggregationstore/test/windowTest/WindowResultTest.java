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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.AggregateWindow;
import stream.scotty.core.AggregateWindowResult;
import stream.scotty.core.windowFunction.AggregateFunction;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

/**
 * Tests that triggered windows are emitted with their final aggregates (#13).
 */
public class WindowResultTest {

    interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
    }

    /**
     * Sum that counts the calls of lower and holds a lambda, which Kryo cannot serialize.
     */
    static class CountingSum implements AggregateFunction<Integer, Integer, Integer> {
        private final SerializableFunction<Integer, Integer> identity = value -> value;
        int lowerCalls = 0;

        @Override
        public Integer lift(Integer inputTuple) {
            return identity.apply(inputTuple);
        }

        @Override
        public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
            return partialAggregate1 + partialAggregate2;
        }

        @Override
        public Integer lower(Integer aggregate) {
            lowerCalls++;
            return aggregate;
        }
    }

    private static SlicingWindowOperator<Integer> createOperator(CountingSum sum) {
        SlicingWindowOperator<Integer> slicingWindowOperator = new SlicingWindowOperator<>(new MemoryStateFactory());
        slicingWindowOperator.addWindowFunction(sum);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(3, 11);
        slicingWindowOperator.processElement(4, 12);
        return slicingWindowOperator;
    }

    @Test
    public void lowerIsComputedOnceWhenWindowIsTriggered() {
        CountingSum sum = new CountingSum();
        SlicingWindowOperator<Integer> slicingWindowOperator = createOperator(sum);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(30);
        Assert.assertEquals(2, sum.lowerCalls);

        WindowAssert.assertContains(resultWindows, 0, 10, 3);
        WindowAssert.assertContains(resultWindows, 10, 20, 7);
        for (AggregateWindow window : resultWindows) {
            Assert.assertTrue(window instanceof AggregateWindowResult);
            window.getAggValues();
        }
        Assert.assertEquals(2, sum.lowerCalls);
    }

    @Test
    public void resultsCanBeCopiedWithKryo() throws IOException {
        // Flink copies emitted windows with Kryo, results must not reference the aggregate function
        SlicingWindowOperator<Integer> slicingWindowOperator = createOperator(new CountingSum());

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(30);
        for (AggregateWindow window : resultWindows) {
            AggregateWindow copy = kryoRoundTrip(window);
            Assert.assertEquals(window, copy);
            Assert.assertEquals(window.getAggValues(), copy.getAggValues());
        }
        WindowAssert.assertContains(resultWindows, 10, 20, 7);
    }

    @SuppressWarnings("unchecked")
    private static <T> T kryoRoundTrip(T object) throws IOException {
        KryoSerializer<T> serializer = new KryoSerializer<>((Class<T>) object.getClass(), new ExecutionConfig());
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(object, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }
}
