package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.state.memory.*;
import org.junit.*;

import java.util.*;

public class SessionWindowOperatorTest {

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
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 23);
        slicingWindowOperator.processElement(4, 31);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));


        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(9, resultWindows.get(0).getAggValues().get(0));
        resultWindows = slicingWindowOperator.processWatermark(80);
        Assert.assertEquals(5, resultWindows.get(0).getAggValues().get(0));

    }

    @Test
    public void inOrderTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 0);
        slicingWindowOperator.processElement(2, 0);
        slicingWindowOperator.processElement(3, 20);
        slicingWindowOperator.processElement(4, 31);
        slicingWindowOperator.processElement(5, 42);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(2).getAggValues().get(0));
    }



    @Test
    public void outOfOrderTestSimpleInsert() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);

        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 15);
        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 12);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(50);
        WindowAssert.assertEquals(resultWindows.get(0), 1, 25,4);
        WindowAssert.assertEquals(resultWindows.get(1), 30, 40,1);
    }

    @Test
    public void outOfOrderTestRightInsert() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);

        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 12);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(50);

        WindowAssert.assertEquals(resultWindows.get(0), 1, 22,4);
        WindowAssert.assertEquals(resultWindows.get(1), 30, 40,1);
    }

    @Test
    public void outOfOrderTestLeftInsert() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);

        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 27);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);
        WindowAssert.assertEquals(resultWindows.get(0), 1, 20,3);

        resultWindows = slicingWindowOperator.processWatermark(50);

        WindowAssert.assertEquals(resultWindows.get(0), 27, 40,2);
    }


    @Test
    public void outOfOrderTestSplitSlice() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);

        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 12);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        WindowAssert.assertEquals(resultWindows.get(0), 1, 11,1);

        resultWindows = slicingWindowOperator.processWatermark(50);
        WindowAssert.assertEquals(resultWindows.get(0), 12, 22,1);
        WindowAssert.assertEquals(resultWindows.get(1), 30, 40,1);

    }


    @Test
    public void outOfOrderTestMergeSlice() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 7);

        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 51);
        slicingWindowOperator.processElement(1, 15);
        slicingWindowOperator.processElement(1, 21);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(70);

        WindowAssert.assertEquals(resultWindows.get(0), 7, 40,4);
        WindowAssert.assertEquals(resultWindows.get(1), 51, 61,1);

    }

    @Test
    public void outOfOrderCombinedSessionTumblingMegeSession() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 40));
        slicingWindowOperator.processElement(1, 7);

        slicingWindowOperator.processElement(1, 22);
        slicingWindowOperator.processElement(1, 51);
        slicingWindowOperator.processElement(1, 15);// merge slice
        slicingWindowOperator.processElement(1, 37);// add new session // split
        //slicingWindowOperator.processElement(1, 21);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(70);

        WindowAssert.assertEquals(resultWindows.get(0), 0, 40,4);
        WindowAssert.assertEquals(resultWindows.get(1), 7, 32,3);
        WindowAssert.assertEquals(resultWindows.get(2), 37, 47,1);
        WindowAssert.assertEquals(resultWindows.get(3), 51, 61,1);

    }



    @Test
    public void outOfOrderCombinedSessionMultiSession() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 5));
        //slicingWindowOperator.processElement(1, 7);
        // events -> 20, 31, 33, 40, 50, 57
        // [20-25, 31-38, 40-45, 50-55, 57-62, 20-30, 31-67]
        slicingWindowOperator.processElement(1, 20);
        slicingWindowOperator.processElement(1, 40);
        slicingWindowOperator.processElement(1, 50);
        slicingWindowOperator.processElement(1, 57);
        slicingWindowOperator.processElement(1, 33);// extend one left
        slicingWindowOperator.processElement(1, 31);// extend one left


        //slicingWindowOperator.processElement(1, 37);// add new session // split
        //slicingWindowOperator.processElement(1, 21);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(70);

        //[20-25, 31-38, 40-45, 50-55, 57-62, 20-30, 31-67]
        WindowAssert.assertContains(resultWindows, 20, 25,1);
        WindowAssert.assertContains(resultWindows, 31, 38,2);
        WindowAssert.assertContains(resultWindows, 40, 45,1);
        WindowAssert.assertContains(resultWindows, 50, 55,1);
        WindowAssert.assertContains(resultWindows, 57, 62,1);
        WindowAssert.assertContains(resultWindows, 20, 30,1);
        WindowAssert.assertContains(resultWindows, 31, 67,5);

    }


}
