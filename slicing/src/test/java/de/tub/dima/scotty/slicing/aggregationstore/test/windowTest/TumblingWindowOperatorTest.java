package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TumblingWindowOperatorTest {

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
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(2, resultWindows.get(1).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(2).getAggValues().get(0));
    }



    @Test
    public void inOrderTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 0);
        slicingWindowOperator.processElement(2, 0);
        slicingWindowOperator.processElement(3, 20);
        slicingWindowOperator.processElement(4, 30);
        slicingWindowOperator.processElement(5, 40);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertFalse( resultWindows.get(1).hasValue());;

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(2).getAggValues().get(0));
    }


    @Test
    public void inOrderTwoWindowsTest() {
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(2, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(2).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(7, resultWindows.get(3).getAggValues().get(0));
    }

    @Test
    public void inOrderTwoWindowsDynamicTest() {

        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(2, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(3, resultWindows.get(2).getAggValues().get(0));

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(7, resultWindows.get(3).getAggValues().get(0));
    }

    @Test
    public void inOrderTwoWindowsDynamicTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 20));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 19);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);


        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));

        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(3, 29);
        slicingWindowOperator.processElement(4, 39);
        slicingWindowOperator.processElement(5, 49);


        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(3).getAggValues().get(0));
        Assert.assertEquals(7, resultWindows.get(0).getAggValues().get(0));
    }


    @Test
    public void outOfOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));
        slicingWindowOperator.processElement(1, 1);

        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 20);
        slicingWindowOperator.processElement(1, 23);
        slicingWindowOperator.processElement(1, 25);

        slicingWindowOperator.processElement(1, 45);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);

        Assert.assertEquals(1, resultWindows.get(0).getAggValues().get(0));
        Assert.assertFalse( resultWindows.get(1).hasValue());;

        resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(1, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(1, resultWindows.get(2).getAggValues().get(0));
    }



    @Test
    public void inOrderTestCount() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 3));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 29);
        slicingWindowOperator.processElement(2, 39);
        slicingWindowOperator.processElement(2, 49);
        slicingWindowOperator.processElement(2, 50);
        slicingWindowOperator.processElement(1, 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(3, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(6, resultWindows.get(1).getAggValues().get(0));
    }

    @Test
    public void outOfOrderOrderTestCount() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 3));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 29);
        slicingWindowOperator.processElement(2, 39);
        // out of order
        slicingWindowOperator.processElement(2, 10);
        slicingWindowOperator.processElement(2, 50);
        slicingWindowOperator.processElement(1, 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(4, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(5, resultWindows.get(1).getAggValues().get(0));
    }

    @Test
    public void outOfOrderOrderTestCount2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate - element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 3));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 5));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 29);
        slicingWindowOperator.processElement(2, 39);
        slicingWindowOperator.processElement(1, 41);
        // out of order
        slicingWindowOperator.processElement(2, 10);
        slicingWindowOperator.processElement(2, 50);
        slicingWindowOperator.processElement(1, 51);
        slicingWindowOperator.processElement(3, 52);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(55);
        Assert.assertEquals(4, resultWindows.get(0).getAggValues().get(0));
        Assert.assertEquals(4, resultWindows.get(1).getAggValues().get(0));
        Assert.assertEquals(6, resultWindows.get(2).getAggValues().get(0));
        Assert.assertEquals(7, resultWindows.get(3).getAggValues().get(0));
    }

    @Test
    public void outOfOrderOrderTestCount3() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 3));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Count, 5));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 29);
        slicingWindowOperator.processElement(2, 39);
        slicingWindowOperator.processElement(1, 41);
        // out of order
        slicingWindowOperator.processElement(2, 10);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(30);
        Assert.assertEquals(4, resultWindows.get(0).getAggValues().get(0));

        slicingWindowOperator.processElement(2, 50);
        slicingWindowOperator.processElement(1, 51);
        slicingWindowOperator.processElement(3, 52);
        resultWindows = slicingWindowOperator.processWatermark(55);

    }

}
