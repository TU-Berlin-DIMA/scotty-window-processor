package stream.scotty.flinkconnector;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.WindowMeasure;

import java.util.ArrayList;
import java.util.List;

public class KeyedScottyWindowOperatorTest {

    private static class Sum implements ReduceAggregateFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> combine(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
            return new Tuple2<>(a.f0, a.f1 + b.f1);
        }
    }

    private static class KeyOf implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    private static List<String> results(KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, Integer>, AggregateWindow<Tuple2<String, Integer>>> harness) {
        List<String> res = new ArrayList<>();
        for (Object o : harness.getOutput()) {
            if (o instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                AggregateWindow<Tuple2<String, Integer>> w = ((StreamRecord<AggregateWindow<Tuple2<String, Integer>>>) o).getValue();
                res.add(w.getStart() + "-" + w.getEnd() + "=" + w.getAggValues().get(0).f1);
            }
        }
        return res;
    }

    /**
     * Sparse keyed events from https://github.com/TU-Berlin-DIMA/scotty-window-processor/issues/61.
     */
    @Test
    public void sparseKeyedEventsTest() throws Exception {
        KeyedScottyWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>> windowOperator =
                new KeyedScottyWindowOperator<>(new Sum());
        windowOperator.addWindow(new SlidingWindow(WindowMeasure.Time, 10, 5));

        KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, Integer>, AggregateWindow<Tuple2<String, Integer>>> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(windowOperator), new KeyOf(), Types.STRING);
        harness.open();

        harness.processElement(new Tuple2<>("user1", 4), 1);
        harness.processWatermark(11);
        harness.processElement(new Tuple2<>("user1", 5), 16);
        harness.processWatermark(25);
        harness.processWatermark(30);
        harness.processElement(new Tuple2<>("user1", 6), 33);
        harness.processWatermark(35);
        harness.processWatermark(40);
        harness.processWatermark(45);
        harness.processWatermark(50);

        List<String> res = results(harness);
        System.out.println(res);
        Assert.assertTrue(res.toString(), res.contains("0-10=4"));
        Assert.assertTrue(res.toString(), res.contains("10-20=5"));
        Assert.assertTrue(res.toString(), res.contains("15-25=5"));
        Assert.assertTrue(res.toString(), res.contains("25-35=6"));
        Assert.assertTrue(res.toString(), res.contains("30-40=6"));
        harness.close();
    }
}
