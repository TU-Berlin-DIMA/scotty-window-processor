package de.tub.dima.scotty.slicing.aggregationstore.test;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.slicing.aggregationstore.test.windowTest.WindowAssert;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class OutOfOrderDemo {

    private SlicingWindowOperator<Integer> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
        // workaround: set this true to output all windows in allowed lateness if tuple in allowed lateness arrived
        // can cause performance issues since many windows are output multiple times!
        this.slicingWindowOperator.setResendWindowsInAllowedLateness(true);
    }

    @Test
    public void simpleOutOfOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2000);
        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2605);
        // tuple is in order, because tuple before has lower timestamp 2800 > 2605
        slicingWindowOperator.processElement(1, 2800);
        // tuple is out-of-order, because the tuple before has a greater timestamp 2400 < 2800
        slicingWindowOperator.processElement(1, 2400);
        slicingWindowOperator.processElement(1, 3700);
        // watermark indicates that no tuple with timestamp lower than 3000 will arrive
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);

        // Scotty outputs the results of all windows that ended before the watermark with timestamp 3000
        // all windows between lastWatermark and currentWatermark are triggered
        // since this is the first watermark, lastWatermark is set to watermark - maxLateness (3000-1000 = 2000)
        WindowAssert.assertContains(resultWindows, 2000, 2010, 1);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 2);
        WindowAssert.assertContains(resultWindows, 2800, 2810, 1);
        WindowAssert.assertContains(resultWindows, 2400, 2410, 1);

        slicingWindowOperator.processElement(2, 3800);
        slicingWindowOperator.processElement(3, 4000);

        //next watermark arrives, Scotty triggers windows that ended between lastWatermark (3000) and currentWatermark (3900)
        resultWindows = slicingWindowOperator.processWatermark(3900);
        WindowAssert.assertContains(resultWindows, 3700, 3710, 1);
        WindowAssert.assertContains(resultWindows, 3800, 3810, 2);
    }

    /* Tests for Allowed Lateness Implementation
    * see OutOfOrderArchitecture.md
    */

    @Test
    public void tupleInAllowedLatenessTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));

        // Scotty stores slices in allowed lateness
        // at the first tuple, slices begin from the tsOfFirstTuple-maxLateness+WindowSize (2600-1000+10 = 1610)
        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2800);
        // when watermark arrives, allowed lateness changes to watermark - maxLateness (3000-1000 = 2000)
        // Scotty stores slices from watermark - maxLateness - maxFixedWindowSize (3000-1000-10 = 1990)
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
        // tuple arrives after watermark, but in allowed lateness, because 2605 > 2000
        slicingWindowOperator.processElement(1, 2605);
        slicingWindowOperator.processElement(1,3010);
        resultWindows = slicingWindowOperator.processWatermark(3100);

        // Scotty outputs changed window result, because resendWindowsInAllowedLateness was set to true
        // updated window results from allowedLateness (i.e., timestamp 2000) until watermark 3100 should be returned
        WindowAssert.assertContains(resultWindows, 2600, 2610, 2);
        // Scotty should only output updated windows, i.e., 2800 - 2810 should not be output
        WindowAssert.assertContains(resultWindows, 3010, 3020, 1);
    }

    @Test
    public void tupleInAllowedLatenessTestAddSession() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2800);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
        // tuple in allowed lateness
        // this tuple should add a new session window
        slicingWindowOperator.processElement(1, 2500);
        slicingWindowOperator.processElement(1, 3010);
        resultWindows = slicingWindowOperator.processWatermark(3100);

        Assert.assertTrue(resultWindows.size() == 2);
        WindowAssert.assertContains(resultWindows, 2500, 2510, 1); // added new session
        //WindowAssert.assertContains(resultWindows, 2600, 2610, 1); // these sessions should not be output
        //WindowAssert.assertContains(resultWindows, 2800, 2810, 1);
        WindowAssert.assertContains(resultWindows, 3010, 3020, 1);
    }

    /*@Test
    public void tupleInAllowedLatenessTestUpdateSession() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2610);
        slicingWindowOperator.processElement(1, 2800);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        WindowAssert.assertContains(resultWindows, 2600, 2620, 2);
        // tuple in allowed lateness has to be inserted in to session 2600-2620, aggregate has to be updated
        slicingWindowOperator.processElement(1, 2605);
        slicingWindowOperator.processElement(1, 3010);
        resultWindows = slicingWindowOperator.processWatermark(3100);

        // output updated session
        WindowAssert.assertContains(resultWindows, 2600, 2620, 3);
    }*/


    /*@Test
    public void tupleInAllowedLatenessTestChangeSession() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2610);
        slicingWindowOperator.processElement(1, 2800);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        WindowAssert.assertContains(resultWindows, 2600, 2620, 2);
        // tuple in allowed lateness that changes the session to be longer from 2600 - 2635
        slicingWindowOperator.processElement(1, 2625);
        slicingWindowOperator.processElement(1, 3010);
        resultWindows = slicingWindowOperator.processWatermark(3100);

        // output changed session
        WindowAssert.assertContains(resultWindows, 2600, 2635, 3);
        WindowAssert.assertContains(resultWindows, 3010, 3020, 1);
    }*/
}
