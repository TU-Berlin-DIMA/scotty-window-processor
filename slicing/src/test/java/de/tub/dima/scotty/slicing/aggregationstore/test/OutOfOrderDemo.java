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

        // Scotty outputs all windows that ended before watermark 3000 starting from watermark - maxlateness (3000-1000)
        WindowAssert.assertContains(resultWindows, 2000, 2010, 1);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 2);
        WindowAssert.assertContains(resultWindows, 2800, 2810, 1);
        WindowAssert.assertContains(resultWindows, 2400, 2410, 1);
    }


    @Test
    public void tupleInAllowedLatenessTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2800);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
        // tuple in allowed lateness
        slicingWindowOperator.processElement(1, 2605);
        slicingWindowOperator.processElement(1,3010);
        resultWindows = slicingWindowOperator.processWatermark(3100);

        // output changed window result
        WindowAssert.assertContains(resultWindows, 2600, 2610, 2);
        WindowAssert.assertContains(resultWindows, 3010, 3020, 1);
    }

    @Test
    public void tupleInAllowedLatenessTestUpdateSession() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2800);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
        //tuple in allowed lateness
        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 3010);
        resultWindows = slicingWindowOperator.processWatermark(3100);

        // output updated session
        WindowAssert.assertContains(resultWindows, 2600, 2610, 2);
    }

    /*@Test
    public void tupleInAllowedLatenessTestChangeSession() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2800);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
        //tuple in allowed lateness
        slicingWindowOperator.processElement(1, 2605);
        slicingWindowOperator.processElement(1, 3010);
        resultWindows = slicingWindowOperator.processWatermark(3100);

        // output changed session
        WindowAssert.assertContains(resultWindows, 2600, 2615, 2);
        WindowAssert.assertContains(resultWindows, 3010, 3020, 1);
    }*/



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
        //WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
        //WindowAssert.assertContains(resultWindows, 2800, 2810, 1);
        WindowAssert.assertContains(resultWindows, 3010, 3020, 1);
    }


}
