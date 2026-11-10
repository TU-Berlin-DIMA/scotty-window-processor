package stream.scotty.flinkconnector;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GlobalScottyWindowOperatorTest {

    private static class Sum implements ReduceAggregateFunction<Integer> {
        @Override
        public Integer combine(Integer a, Integer b) {
            return a + b;
        }
    }

    private static String toString(AggregateWindow<Integer> w) {
        return w.getStart() + "-" + w.getEnd() + "=" + w.getAggValues().get(0);
    }

    /**
     * Returns the windows with values and the watermarks in the order of the operator output.
     */
    private static List<String> output(OneInputStreamOperatorTestHarness<Integer, AggregateWindow<Integer>> harness) {
        List<String> res = new ArrayList<>();
        for (Object o : harness.getOutput()) {
            if (o instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                AggregateWindow<Integer> w = ((StreamRecord<AggregateWindow<Integer>>) o).getValue();
                if (w.hasValue())
                    res.add(toString(w));
            } else if (o instanceof Watermark) {
                res.add("W" + ((Watermark) o).getTimestamp());
            }
        }
        return res;
    }

    /**
     * Sparse events from https://github.com/TU-Berlin-DIMA/scotty-window-processor/issues/61 without keys.
     */
    @Test
    public void sparseEventsTest() throws Exception {
        GlobalScottyWindowOperator<Integer, Integer> windowOperator = new GlobalScottyWindowOperator<>(new Sum());
        windowOperator.addWindow(new SlidingWindow(WindowMeasure.Time, 10, 5));

        OneInputStreamOperatorTestHarness<Integer, AggregateWindow<Integer>> harness = new OneInputStreamOperatorTestHarness<>(windowOperator);
        harness.open();

        harness.processElement(4, 1);
        harness.processWatermark(11);
        harness.processElement(5, 16);
        harness.processWatermark(25);
        harness.processWatermark(30);
        harness.processElement(6, 33);
        harness.processWatermark(35);
        harness.processWatermark(40);
        harness.processWatermark(45);

        // windows are emitted before the watermark that triggered them is forwarded
        Assert.assertEquals(Arrays.asList("0-10=4", "W11", "15-25=5", "10-20=5", "W25", "W30", "25-35=6", "W35", "30-40=6", "W40", "W45"),
                output(harness));
        harness.close();
    }

    @Test
    public void endOfStreamTest() throws Exception {
        GlobalScottyWindowOperator<Integer, Integer> windowOperator = new GlobalScottyWindowOperator<>(new Sum());
        windowOperator.addWindow(new TumblingWindow(WindowMeasure.Time, 10));

        OneInputStreamOperatorTestHarness<Integer, AggregateWindow<Integer>> harness = new OneInputStreamOperatorTestHarness<>(windowOperator);
        harness.open();

        harness.processElement(1, 1);
        harness.processWatermark(5);
        harness.processElement(2, 12);
        harness.processWatermark(Long.MAX_VALUE);

        Assert.assertEquals(Arrays.asList("W5", "0-10=1", "10-20=2", "W" + Long.MAX_VALUE), output(harness));
        harness.close();
    }

    private static final List<String> sinkResults = Collections.synchronizedList(new ArrayList<>());

    @Test
    public void applyToTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Integer> stream = env.addSource(new SourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) {
                ctx.collectWithTimestamp(1, 1);
                ctx.emitWatermark(new Watermark(11));
                ctx.collectWithTimestamp(2, 16);
                ctx.emitWatermark(new Watermark(25));
            }

            @Override
            public void cancel() {
            }
        });

        sinkResults.clear();
        new GlobalScottyWindowOperator<Integer, Integer>(new Sum())
                .addWindow(new TumblingWindow(WindowMeasure.Time, 10))
                .addWindow(new TumblingWindow(WindowMeasure.Time, 100))
                .applyTo(stream)
                .addSink(new SinkFunction<AggregateWindow<Integer>>() {
                    @Override
                    public void invoke(AggregateWindow<Integer> value, Context context) {
                        if (value.hasValue())
                            sinkResults.add(GlobalScottyWindowOperatorTest.toString(value));
                    }
                });
        env.execute();

        // the window 0-100 is emitted with the final watermark at the end of the stream
        Assert.assertEquals(Arrays.asList("0-10=1", "10-20=2", "0-100=3"), sinkResults);
    }
}
