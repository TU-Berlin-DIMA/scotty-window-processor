package stream.scotty.slicing.aggregationstore.test.windowTest;

import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import stream.scotty.core.windowType.*;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link SlicingWindowOperator#hasPendingWindows()} and the end of stream watermark Long.MAX_VALUE.
 */
public class PendingWindowsTest {

    private SlicingWindowOperator<Integer> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
    }

    private static List<AggregateWindow> withValue(List<AggregateWindow> windows) {
        List<AggregateWindow> result = new ArrayList<>();
        for (AggregateWindow window : windows) {
            if (window.hasValue())
                result.add(window);
        }
        return result;
    }

    @Test
    public void thresholdFrameTest() {
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(5));

        slicingWindowOperator.processElement(7, 1);
        slicingWindowOperator.processElement(8, 2);
        // the frame is still open, only a tuple below the threshold can close it
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());
        Assert.assertTrue(withValue(slicingWindowOperator.processWatermark(10)).isEmpty());
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());

        slicingWindowOperator.processElement(1, 11);
        Assert.assertTrue(slicingWindowOperator.hasPendingWindows());

        List<AggregateWindow> resultWindows = withValue(slicingWindowOperator.processWatermark(20));
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 1, 11, 15);
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());
    }

    @Test
    public void slidingWindowTest() {
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 5));
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 12);
        Assert.assertTrue(slicingWindowOperator.hasPendingWindows());

        slicingWindowOperator.processWatermark(15);
        Assert.assertTrue(slicingWindowOperator.hasPendingWindows());

        List<AggregateWindow> resultWindows = withValue(slicingWindowOperator.processWatermark(22));
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 10, 20, 2);
        // the window 15-25 does not contain tuples
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());

        slicingWindowOperator.processElement(3, 30);
        Assert.assertTrue(slicingWindowOperator.hasPendingWindows());
    }

    @Test
    public void sessionWindowTest() {
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 5);
        Assert.assertTrue(slicingWindowOperator.hasPendingWindows());

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(1000);
        WindowAssert.assertEquals(resultWindows.get(0), 1, 15, 3);
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());

        slicingWindowOperator.processElement(3, 2000);
        Assert.assertTrue(slicingWindowOperator.hasPendingWindows());
    }

    @Test
    public void punctuationWindowTest() {
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(0));
        slicingWindowOperator.processElement(1, 1);
        // the window after the last punctuation can only be triggered by the next punctuation
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());

        slicingWindowOperator.processElement(0, 10); //Punctuation
        slicingWindowOperator.processElement(2, 12);
        Assert.assertTrue(slicingWindowOperator.hasPendingWindows());

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(11);
        WindowAssert.assertEquals(resultWindows.get(0), 1, 10, 1);
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());
    }

    @Test
    public void endOfStreamWatermarkTest() {
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 5));
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 100));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);

        List<AggregateWindow> resultWindows = withValue(slicingWindowOperator.processWatermark(12));
        // tumbling and sliding window 0-10
        Assert.assertEquals(2, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 0, 10, 1);
        WindowAssert.assertEquals(resultWindows.get(1), 0, 10, 1);

        resultWindows = withValue(slicingWindowOperator.processWatermark(Long.MAX_VALUE));
        // tumbling and sliding window 10-20, sliding window 15-25 and the session window
        Assert.assertEquals(4, resultWindows.size());
        WindowAssert.assertContains(resultWindows, 10, 20, 2);
        WindowAssert.assertContains(resultWindows, 15, 25, 2);
        WindowAssert.assertContains(resultWindows, 1, 119, 3);
        Assert.assertFalse(slicingWindowOperator.hasPendingWindows());
    }
}
