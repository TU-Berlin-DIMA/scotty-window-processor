package stream.scotty.slicing.aggregationstore.test.windowTest;

import stream.scotty.core.*;
import stream.scotty.core.windowFunction.*;
import stream.scotty.core.windowType.*;
import stream.scotty.slicing.*;
import stream.scotty.state.memory.*;
import org.junit.*;

import java.util.*;

/**
 * Regression tests for https://github.com/TU-Berlin-DIMA/scotty-window-processor/issues/46:
 * an out-of-order tuple in allowed lateness, which is older than the oldest slice,
 * caused an ArrayIndexOutOfBoundsException in LazyAggregateStore.getSlice.
 * Such tuples have to be added to new slices in front of the oldest slice
 * and have to be counted in all windows which contain them.
 */
public class OutOfOrderBeforeFirstSliceTest {

    private SlicingWindowOperator<Integer> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
    }

    @Test
    public void tumblingWindowBeforeWatermarkTest() {
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.setMaxLateness(20);

        // first slice starts at 90 (first window edge after 100 - maxLateness)
        slicingWindowOperator.processElement(1, 100);
        // in allowed lateness (100 - 20 = 80), but older than the first slice
        slicingWindowOperator.processElement(2, 85);
        slicingWindowOperator.processElement(4, 95);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(101);
        assertWindow(resultWindows, 80, 90, 2);
        assertWindow(resultWindows, 90, 100, 4);
    }

    @Test
    public void tumblingWindowAfterWatermarkTest() {
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.setMaxLateness(20);
        slicingWindowOperator.setResendWindowsInAllowedLateness(true);

        // first slice starts at 90
        slicingWindowOperator.processElement(1, 100);
        slicingWindowOperator.processWatermark(101);
        // in allowed lateness (101 - 20 = 81 < window end 90), but older than the first slice
        slicingWindowOperator.processElement(2, 85);
        slicingWindowOperator.processElement(4, 105);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(120);
        assertWindow(resultWindows, 80, 90, 2);
        assertWindow(resultWindows, 100, 110, 5);
    }

    @Test
    public void tupleOutsideAllowedLatenessBeforeWatermarkTest() {
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.setMaxLateness(20);

        slicingWindowOperator.processElement(1, 100);
        // outside of allowed lateness: ts is not bigger than first timestamp - maxLateness (100 - 20 = 80)
        slicingWindowOperator.processElement(2, 80);
        // in allowed lateness
        slicingWindowOperator.processElement(4, 81);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(101);
        assertWindow(resultWindows, 80, 90, 4);
    }

    @Test
    public void slidingWindowAfterWatermarkTest() {
        // reproduction of the out-of-order case of #46 with a small sliding window
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 5));
        slicingWindowOperator.setMaxLateness(20);
        slicingWindowOperator.setResendWindowsInAllowedLateness(true);

        // first slice starts at 85
        slicingWindowOperator.processElement(1, 100);
        slicingWindowOperator.processWatermark(101);
        // in allowed lateness, but older than the first slice
        slicingWindowOperator.processElement(1, 82);
        slicingWindowOperator.processElement(2, 107);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(120);
        assertWindow(resultWindows, 75, 85, 1);
        assertWindow(resultWindows, 80, 90, 1);
        assertWindow(resultWindows, 95, 105, 1);
        assertWindow(resultWindows, 100, 110, 3);
        assertWindow(resultWindows, 105, 115, 2);
    }

    @Test
    public void slidingWindowLargeLatenessBeforeWatermarkTest() {
        // window configuration of #46 with maxLateness >= window size
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 600000, 60000));
        slicingWindowOperator.setMaxLateness(600000);

        // first slice starts at 3060000 (first window edge after 3600000 - maxLateness)
        slicingWindowOperator.processElement(1, 3600000);
        // in allowed lateness, but older than the first slice
        slicingWindowOperator.processElement(2, 3030000);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3660000);
        for (int windowStart = 2520000; windowStart <= 3000000; windowStart += 60000) {
            assertWindow(resultWindows, windowStart, windowStart + 600000, 2);
        }
        assertWindow(resultWindows, 3060000, 3660000, 1);
    }

    @Test
    public void slidingWindowLargeLatenessAfterWatermarkTest() {
        // window configuration of #46 with maxLateness >= window size
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 600000, 60000));
        slicingWindowOperator.setMaxLateness(600000);
        slicingWindowOperator.setResendWindowsInAllowedLateness(true);

        // first slice starts at 3060000
        slicingWindowOperator.processElement(1, 3600000);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3660000);
        assertWindow(resultWindows, 3060000, 3660000, 1);

        // in allowed lateness (3660000 - 600000 = 3060000 < window ends), but older than the first slice
        slicingWindowOperator.processElement(2, 3030000);

        resultWindows = slicingWindowOperator.processWatermark(3720000);
        // windows ending after 3060000 which contain 3030000 are updated
        for (int windowStart = 2520000; windowStart <= 3000000; windowStart += 60000) {
            assertWindow(resultWindows, windowStart, windowStart + 600000, 2);
        }
        assertWindow(resultWindows, 3060000, 3660000, 1);
        assertWindow(resultWindows, 3120000, 3720000, 1);
    }

    @Test
    public void countWindowBeforeWatermarkTest() {
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 3));

        // first slice starts at the first tuple
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(2, 20);
        slicingWindowOperator.processElement(3, 30);
        slicingWindowOperator.processElement(4, 40);
        // in allowed lateness, but older than the first slice -> first tuple in count order
        slicingWindowOperator.processElement(5, 5);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(50);
        // count order: 5, 1, 2 | 3, 4
        assertWindow(resultWindows, 0, 3, 8);
        assertWindow(resultWindows, 3, 6, 7);
    }

    @Test
    public void countWindowAfterWatermarkTest() {
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 3));

        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(2, 20);
        slicingWindowOperator.processElement(3, 30);
        slicingWindowOperator.processElement(4, 40);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(45);
        assertWindow(resultWindows, 0, 3, 6);

        // in allowed lateness, but older than the first slice -> first tuple in count order
        slicingWindowOperator.processElement(5, 5);
        slicingWindowOperator.processElement(6, 60);
        slicingWindowOperator.processElement(7, 70);

        resultWindows = slicingWindowOperator.processWatermark(80);
        // count order: 5, 1, 2 | 3, 4, 6 | 7
        // count-based windows are not resend, so only the shifted window [3, 6) is output
        assertWindow(resultWindows, 3, 6, 13);
    }

    @Test
    public void sessionWindowBeforeWatermarkTest() {
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        // first slice starts at the first tuple
        slicingWindowOperator.processElement(1, 100);
        // in allowed lateness, older than the first slice and extends the session start
        slicingWindowOperator.processElement(2, 95);
        slicingWindowOperator.processElement(4, 120);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(200);
        Assert.assertEquals(2, resultWindows.size());
        assertWindow(resultWindows, 95, 110, 3);
        assertWindow(resultWindows, 120, 130, 4);
    }

    @Test
    public void sessionWindowAfterWatermarkTest() {
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.setResendWindowsInAllowedLateness(true);

        slicingWindowOperator.processElement(1, 100);
        // session [100, 110) is not finished yet
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(105);
        Assert.assertTrue(resultWindows.isEmpty());

        // in allowed lateness, older than the first slice and extends the session start
        slicingWindowOperator.processElement(2, 95);

        resultWindows = slicingWindowOperator.processWatermark(200);
        Assert.assertEquals(1, resultWindows.size());
        assertWindow(resultWindows, 95, 110, 3);
    }

    @Test
    public void multipleTimeWindowsTest() {
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 20, 5));
        slicingWindowOperator.setMaxLateness(50);
        slicingWindowOperator.setResendWindowsInAllowedLateness(true);

        // first slice starts at 55
        slicingWindowOperator.processElement(1, 100);
        // in allowed lateness (100 - 50 = 50), but older than the first slice
        slicingWindowOperator.processElement(2, 52);
        slicingWindowOperator.processElement(4, 97);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(101);
        assertWindow(resultWindows, 50, 60, 2);
        assertWindow(resultWindows, 90, 100, 4);
        assertWindow(resultWindows, 35, 55, 2);
        assertWindow(resultWindows, 40, 60, 2);
        assertWindow(resultWindows, 45, 65, 2);
        assertWindow(resultWindows, 50, 70, 2);
        assertWindow(resultWindows, 80, 100, 4);

        // in allowed lateness (101 - 50 = 51 < end of sliding windows [35, 55) and [40, 60)),
        // but older than the first slice: new slices have to be split at 45 and 50
        slicingWindowOperator.processElement(8, 41);
        slicingWindowOperator.processElement(16, 103);

        resultWindows = slicingWindowOperator.processWatermark(110);
        assertWindow(resultWindows, 50, 60, 2);
        assertWindow(resultWindows, 90, 100, 4);
        assertWindow(resultWindows, 100, 110, 17);
        assertWindow(resultWindows, 35, 55, 10);
        assertWindow(resultWindows, 40, 60, 10);
        assertWindow(resultWindows, 45, 65, 2);
        assertWindow(resultWindows, 50, 70, 2);
        assertWindow(resultWindows, 80, 100, 4);
        assertWindow(resultWindows, 85, 105, 21);
        assertWindow(resultWindows, 90, 110, 21);
    }

    @Test
    public void sessionAndTumblingWindowTest() {
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 3));
        slicingWindowOperator.setMaxLateness(25);
        slicingWindowOperator.setResendWindowsInAllowedLateness(true);

        // first slice starts at 80
        slicingWindowOperator.processElement(1, 100);
        slicingWindowOperator.processWatermark(101);

        // in allowed lateness, older than the first slice and starts a new session:
        // new slices have to be split at the tumbling window edge 70
        slicingWindowOperator.processElement(2, 68);
        // extends the new session and belongs to the tumbling window [70, 80)
        slicingWindowOperator.processElement(4, 70);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(110);
        assertWindow(resultWindows, 70, 80, 4);
        assertWindow(resultWindows, 100, 110, 1);
        assertWindow(resultWindows, 68, 73, 6);
        assertWindow(resultWindows, 100, 103, 1);
    }

    private static void assertWindow(List<AggregateWindow> resultWindows, long start, long end, Object value) {
        for (AggregateWindow window : resultWindows) {
            if (window.getStart() == start && window.getEnd() == end) {
                Assert.assertTrue("window " + start + "-" + end + " has no value", window.hasValue());
                Assert.assertEquals("window " + start + "-" + end, value, window.getAggValues().get(0));
                return;
            }
        }
        Assert.fail("window " + start + "-" + end + " not in " + resultWindows);
    }
}
