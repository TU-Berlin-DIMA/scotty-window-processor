package stream.scotty.slicing.aggregationstore.test.windowTest;

import stream.scotty.core.*;
import stream.scotty.core.windowFunction.*;
import stream.scotty.core.windowType.*;
import stream.scotty.slicing.*;
import stream.scotty.state.memory.*;
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
    public void inOrderTestClean() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10000));
        slicingWindowOperator.processElement(1, 1000);
        slicingWindowOperator.processElement(2, 19000);
        slicingWindowOperator.processElement(3, 23000);
        slicingWindowOperator.processElement(4, 31000);
        slicingWindowOperator.processElement(5, 49000);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22000);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));


        resultWindows = slicingWindowOperator.processWatermark(55000);
        Assert.assertEquals(9, resultWindows.get(0).getAggValues().get(0));
        resultWindows = slicingWindowOperator.processWatermark(80000);
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


    @Test
    public void tupleOutsideAllowedLatenessTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2700);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        slicingWindowOperator.processElement(1, 3700);

        // tuple outside of allowed lateness: tuple ts is smaller than watermark - maxLateness (here: 3000-1000 = 2000)
        // this tuple should not start a new session or be added to any slice
        slicingWindowOperator.processElement(1, 1999);

        Assert.assertTrue(resultWindows.size() == 2);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
        WindowAssert.assertContains(resultWindows, 2700, 2710, 1);
    }

    @Test
    public void tupleInsideAllowedLatenessTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);
        slicingWindowOperator.processElement(1, 3700);

        // tuple inside of allowed lateness: tuple ts is bigger than watermark - maxLateness (here: 3000-1000 = 2000)
        // this tuple should add a new session window
        slicingWindowOperator.processElement(1, 2500);

        resultWindows = slicingWindowOperator.processWatermark(4000);

        Assert.assertTrue(resultWindows.size() == 2);
        WindowAssert.assertContains(resultWindows, 2500, 2510, 1);
        WindowAssert.assertContains(resultWindows, 3700, 3710, 1);
    }

    @Test
    public void tupleOutsideAllowedLatenessTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SessionWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 2600);
        slicingWindowOperator.processElement(1, 2700);

        // tuple outside of allowed lateness: tuple ts is smaller than firstTimestamp - maxLateness (here: 2600-1000 = 1600)
        // this tuple should not be added to any slice
        slicingWindowOperator.processElement(1, 1500);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(3000);

        Assert.assertTrue(resultWindows.size() == 2);
        WindowAssert.assertContains(resultWindows, 2600, 2610, 1);
        WindowAssert.assertContains(resultWindows, 2700, 2710, 1);
    }

}
