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
package stream.scotty.stormconnector;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.builtin.Sum;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class KeyedScottyWindowOperatorTest {

    /**
     * Records the emitted (key, window) values instead of sending them to a Storm topology.
     */
    static class RecordingCollector extends BasicOutputCollector {
        final List<String> emitted = new ArrayList<>();

        RecordingCollector() {
            super(null);
        }

        @Override
        public List<Integer> emit(List<Object> tuple) {
            AggregateWindow<?> window = (AggregateWindow<?>) tuple.get(1);
            emitted.add(tuple.get(0) + ":" + window.getStart() + "-" + window.getEnd() + "=" + window.getAggValues());
            return Collections.emptyList();
        }
    }

    private KeyedScottyWindowOperator<String, Integer> operator;
    private RecordingCollector collector;

    @Before
    public void setup() {
        operator = new KeyedScottyWindowOperator<>(Sum.<Integer>create(), 1000);
        operator.addWindow(new TumblingWindow(WindowMeasure.Time, 1000));
        operator.prepare(new HashMap(), null);
        collector = new RecordingCollector();
    }

    /**
     * Creates an input tuple in the format of the connector: (key, value, timestamp).
     */
    private static Tuple tuple(String key, int value, long timestamp) {
        List<Object> values = Arrays.asList(key, value, timestamp);
        return (Tuple) Proxy.newProxyInstance(Tuple.class.getClassLoader(), new Class<?>[]{Tuple.class}, (proxy, method, args) -> {
            switch (method.getName()) {
                case "getValue":
                    return values.get((Integer) args[0]);
                case "getLong":
                    return (Long) values.get((Integer) args[0]);
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        });
    }

    private List<String> emittedSorted() {
        List<String> emitted = new ArrayList<>(collector.emitted);
        Collections.sort(emitted);
        return emitted;
    }

    @Test
    public void windowsAreEmittedWithTheirKey() {
        operator.execute(tuple("a", 1, 100), collector);
        operator.execute(tuple("b", 2, 200), collector);
        operator.execute(tuple("a", 3, 300), collector);
        // the tuple of key c processes the watermark and triggers the windows of a and b
        operator.execute(tuple("c", 4, 1500), collector);

        Assert.assertEquals(Arrays.asList("a:0-1000=[4]", "b:0-1000=[2]"), emittedSorted());
    }

    @Test
    public void emptyWindowsAreNotEmitted() {
        operator.execute(tuple("a", 1, 100), collector);
        operator.execute(tuple("a", 5, 1500), collector);
        operator.execute(tuple("a", 7, 5000), collector);

        // the windows 2000-3000 and 3000-4000 do not contain tuples
        Assert.assertEquals(Arrays.asList("a:0-1000=[1]", "a:1000-2000=[5]"), emittedSorted());
    }
}
