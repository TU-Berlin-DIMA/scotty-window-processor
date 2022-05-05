package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.state.memory.*;
import org.junit.*;

import java.util.*;

public class SlidingWindowOperatorTest {

    private SlicingWindowOperator<Integer> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
    }

    @Test
    public void inOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 5));
        // 1,10 ; 5-15; 20-30---
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(2).getAggValues().get(0));
        Assert.assertFalse( resultWindows.get(1).hasValue());
        Assert.assertEquals(2, resultWindows.get(0).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(5, resultWindows.get(0).getAggValues().get(0)); // 44 - 55
        Assert.assertEquals(5, resultWindows.get(1).getAggValues().get(0)); // 40 - 50
        Assert.assertEquals(4, resultWindows.get(2).getAggValues().get(0)); // 35 - 45
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0)); // 30 - 40
        Assert.assertEquals(3, resultWindows.get(4).getAggValues().get(0)); // 25 - 35
        Assert.assertEquals(3, resultWindows.get(5).getAggValues().get(0)); // 20 - 30
        Assert.assertEquals(2, resultWindows.get(6).getAggValues().get(0)); // 15 - 25
    }

    @Test
    public void inOrderTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 5));
        slicingWindowOperator.processElement(1, 0);
        slicingWindowOperator.processElement(2, 0);
        slicingWindowOperator.processElement(3, 20);
        slicingWindowOperator.processElement(4, 30);
        slicingWindowOperator.processElement(5, 40);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertFalse( resultWindows.get(0).hasValue()); // 10 - 20
        Assert.assertFalse( resultWindows.get(1).hasValue());   // 5 - 15
        Assert.assertEquals(3, resultWindows.get(2).getAggValues().get(0));   // 0 - 10

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertFalse( resultWindows.get(0).hasValue()); // 44 - 55
        Assert.assertEquals(5, resultWindows.get(1).getAggValues().get(0)); // 40 - 50
        Assert.assertEquals(5, resultWindows.get(2).getAggValues().get(0)); // 35 - 45
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0)); // 30 - 40
        Assert.assertEquals(4, resultWindows.get(4).getAggValues().get(0)); // 25 - 35
        Assert.assertEquals(3, resultWindows.get(5).getAggValues().get(0)); // 20 - 30
        Assert.assertEquals(3, resultWindows.get(6).getAggValues().get(0)); // 15 - 25
    }


    @Test
    public void inOrderTwoWindowsTest() {
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(2, resultWindows.get(0).getAggValues().get(0)); // 10 - 20
        Assert.assertFalse( resultWindows.get(1).hasValue()); // 5 - 15
        Assert.assertEquals(1, resultWindows.get(2).getAggValues().get(0)); // 0 - 10
        Assert.assertEquals(3, resultWindows.get(3).getAggValues().get(0)); // 0  - 20

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(5, resultWindows.get(0).getAggValues().get(0)); // 45 - 55
        Assert.assertEquals(5, resultWindows.get(1).getAggValues().get(0)); // 40 - 50
        Assert.assertEquals(4, resultWindows.get(2).getAggValues().get(0)); // 35 - 45
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0)); // 30 - 40
        Assert.assertEquals(3, resultWindows.get(4).getAggValues().get(0)); // 25 - 35
        Assert.assertEquals(3, resultWindows.get(5).getAggValues().get(0)); // 20 - 30
        Assert.assertEquals(2, resultWindows.get(6).getAggValues().get(0)); // 15 - 25
        Assert.assertEquals(7, resultWindows.get(7).getAggValues().get(0)); // 20 - 40
    }

    @Test
    public void inOrderTwoWindowsDynamicTest() {

        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(2, resultWindows.get(0).getAggValues().get(0));
        Assert.assertFalse( resultWindows.get(1).hasValue());;
        Assert.assertEquals(1, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(3).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(5, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(4).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(5).getAggValues().get(0)); // 45 - 55
        Assert.assertEquals(2, resultWindows.get(6).getAggValues().get(0)); // 45 - 55
        Assert.assertEquals(7, resultWindows.get(7).getAggValues().get(0)); // 20 - 40
    }

    @Test
    public void inOrderTwoWindowsDynamicTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);


        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));

        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));


        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);


        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(7, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(4).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(5).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(6).getAggValues().get(0));
    }

    @Test
    public void slideTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10, 3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 11);
        slicingWindowOperator.processElement(1, 12);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 15);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(2, 21);
        slicingWindowOperator.processElement(1, 22);
        slicingWindowOperator.processElement(1, 23);
        slicingWindowOperator.processElement(1, 24);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 36);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        WindowAssert.assertContains(resultWindows, 0, 10, 1);
        WindowAssert.assertContains(resultWindows, 3, 13, 3);
        WindowAssert.assertContains(resultWindows, 6, 16, 6);
        WindowAssert.assertContains(resultWindows, 9, 19, 6);
        WindowAssert.assertContains(resultWindows, 12, 22, 7);

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertContains(resultWindows, 15, 25, 7);
        WindowAssert.assertContains(resultWindows, 18, 28, 6);
        WindowAssert.assertContains(resultWindows, 21, 31, 8);
        WindowAssert.assertContains(resultWindows, 24, 34, 4);
        WindowAssert.assertContains(resultWindows, 27, 37, 7);
    }


    @Test
    public void outOfOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));
        slicingWindowOperator.processElement(1, 1);

        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 20);
        slicingWindowOperator.processElement(1, 23);
        slicingWindowOperator.processElement(1, 25);

        slicingWindowOperator.processElement(1, 45);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertFalse( resultWindows.get(0).hasValue()); // 10 - 20
        Assert.assertFalse( resultWindows.get(1).hasValue());;                // 5 - 15
        Assert.assertEquals(1, resultWindows.get(2).getAggValues().get(0));                // 0 - 10

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));       // 45 - 55
        Assert.assertEquals(1,  resultWindows.get(1).getAggValues().get(0));      // 40 - 50
        Assert.assertFalse( resultWindows.get(2).hasValue());      // 35 - 45
        Assert.assertEquals(1, resultWindows.get(3).getAggValues().get(0));      // 30 - 40
        Assert.assertEquals( 2,resultWindows.get(4).getAggValues().get(0));                     // 25 - 35
        Assert.assertEquals(3, resultWindows.get(5).getAggValues().get(0));      // 20 - 30
        Assert.assertEquals(2, resultWindows.get(6).getAggValues().get(0));      // 15 - 25
    }


    @Test
    public void tupleOutsideAllowedLatenessTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));

        slicingWindowOperator.processElement(1, 2600);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        slicingWindowOperator.processElement(1, 3700);

        // tuple outside of allowed lateness: tuple ts is smaller than watermark - maxLateness - maxFixedWindowSize (here: 3000-1000-10 = 1990)
        // this tuple should not be added to any slice
        slicingWindowOperator.processElement(1, 1980);

        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
    }


    @Test
    public void tupleOutsideAllowedLatenessTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time, 10,5));

        slicingWindowOperator.processElement(1, 2600);

        // no watermark before tuple outside of allowed lateness
        // ts is smaller than ts - maxLateness + slide - (ts - maxLateness) % slide (here: 2600-1000+5 = 1605)
        // this tuple should not be added to any slice
        slicingWindowOperator.processElement(1, 1500);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);

        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
    }


}
