package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.SlideByTupleWindow;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SlideByTupleWindowTest {

    private SlicingWindowOperator<Integer> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<Integer>(stateFactory);
    }

    @Test
    public void inOrderSlide1() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 1));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 24);
        slicingWindowOperator.processElement(1, 24);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        // Assert for slide = 1
        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 4);
        WindowAssert.assertEquals(resultWindows.get(1),3, 13, 3);
        WindowAssert.assertEquals(resultWindows.get(2),6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(3),8, 18, 3);
        WindowAssert.assertEquals(resultWindows.get(4),13, 23, 2);
        WindowAssert.assertEquals(resultWindows.get(5),17, 27, 3);
        WindowAssert.assertEquals(resultWindows.get(6),24, 34, 2);
        WindowAssert.assertEquals(resultWindows.get(7),24, 34, 2);
    }

    @Test
    public void inOrderSlide2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 24);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        // Assert for slide = 2
        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 4);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 2);
        WindowAssert.assertEquals(resultWindows.get(3),24, 34, 1);
    }


    @Test
    public void inOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 5));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 4);
        slicingWindowOperator.processElement(1, 5);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 12);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 16);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 18);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 21);
        slicingWindowOperator.processElement(1, 22);
        slicingWindowOperator.processElement(1, 24);
        slicingWindowOperator.processElement(1, 25);
        slicingWindowOperator.processElement(1, 27);
        slicingWindowOperator.processElement(1, 30);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 9);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 7);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 8);
        WindowAssert.assertEquals(resultWindows.get(3),19, 29, 6);
        WindowAssert.assertEquals(resultWindows.get(4),27, 37, 2);
    }

    @Test
    public void inOrderTestOutlier() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 24); //outlier, should not be contained in any window
        slicingWindowOperator.processElement(1, 30);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 4);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 1);
        WindowAssert.assertEquals(resultWindows.get(3),30, 40, 1);
    }

    @Test
    public void inOrderTwoWindowsSize() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        //windows with different sizes
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(20, 2));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1,23);
        slicingWindowOperator.processElement(1, 24);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(45);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 4);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 2);
        WindowAssert.assertEquals(resultWindows.get(3),23, 33, 2);
        WindowAssert.assertEquals(resultWindows.get(4),1, 21, 6);
        WindowAssert.assertEquals(resultWindows.get(5),6, 26, 6);
        WindowAssert.assertEquals(resultWindows.get(6),13, 33, 4);
        WindowAssert.assertEquals(resultWindows.get(7),23, 43, 2);

    }

    @Test
    public void inOrderTwoWindowsSlide() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        //windows with different slides
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 3));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 24);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        /*Assert for two windows slide = 2 and slide = 3*/
        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 4);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 2);
        WindowAssert.assertEquals(resultWindows.get(3),24, 34, 1);
        WindowAssert.assertEquals(resultWindows.get(4),1, 11, 4);
        WindowAssert.assertEquals(resultWindows.get(5),8, 18, 3);
        WindowAssert.assertEquals(resultWindows.get(6),24, 34, 1);
    }

    @Test
    public void inOrderTwoWindowsTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        //windows with different size and slide
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(20, 5));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1,23);
        slicingWindowOperator.processElement(1, 24);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(45);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 4);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 2);
        WindowAssert.assertEquals(resultWindows.get(3),23, 33, 2);
        WindowAssert.assertEquals(resultWindows.get(4),1, 21, 6);
        WindowAssert.assertEquals(resultWindows.get(5),17, 37, 3);
    }

    @Test
    public void outOfOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 6); // out of order Tuple, shift start of window after
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 24);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 4);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 2);
    }

    @Test
    public void outOfOrderTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 21);
        slicingWindowOperator.processElement(1, 25);
        slicingWindowOperator.processElement(1,26);
        slicingWindowOperator.processElement(1, 16); // out of order Tuple, shift start of two windows before

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        WindowAssert.assertEquals(resultWindows.get(0), 1, 11, 3);
        WindowAssert.assertEquals(resultWindows.get(1), 8, 18, 4);
        WindowAssert.assertEquals(resultWindows.get(2), 14, 24, 4);
        WindowAssert.assertEquals(resultWindows.get(3), 19,29,4);
        WindowAssert.assertEquals(resultWindows.get(4),25,35,2);
    }

    @Test
    public void outOfOrderTest3() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 5));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 4);
        slicingWindowOperator.processElement(1, 5);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 12);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 16);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 18);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 21);
        slicingWindowOperator.processElement(1, 22);
        slicingWindowOperator.processElement(1, 24);
        slicingWindowOperator.processElement(1, 25);
        slicingWindowOperator.processElement(1, 27);
        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1,7); //out-of-order tuple, shift three windows

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 10);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 8);
        WindowAssert.assertEquals(resultWindows.get(2),12, 22, 8);
        WindowAssert.assertEquals(resultWindows.get(3),18, 28, 7);
        WindowAssert.assertEquals(resultWindows.get(4),25, 35, 3);
    }

    @Test
    public void outOfOrderTest4() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 5));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 4);
        slicingWindowOperator.processElement(1, 5);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 12);
        slicingWindowOperator.processElement(1, 13); //start of window
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 16); //count=2
        slicingWindowOperator.processElement(1, 22);
        slicingWindowOperator.processElement(1, 24); //count=4 before out of order tuples -> no new window starts before out-of-order tuples arrive

        //out-of-order tuples
        slicingWindowOperator.processElement(1, 17); //count=3 if in-order after 16
        slicingWindowOperator.processElement(1, 18);
        slicingWindowOperator.processElement(1, 19); //count=5 -> start of new window instead of shift
        slicingWindowOperator.processElement(1, 21); //count=6

        slicingWindowOperator.processElement(1, 25);
        slicingWindowOperator.processElement(1, 27);
        slicingWindowOperator.processElement(1, 30);


        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 9);
        WindowAssert.assertEquals(resultWindows.get(1),6, 16, 7);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 8);
        WindowAssert.assertEquals(resultWindows.get(3),19, 29, 6);
        WindowAssert.assertEquals(resultWindows.get(4),27, 37, 2);
    }

    @Test
    public void outOfOrderTest5() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 1));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 12);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 16);
        slicingWindowOperator.processElement(1, 18);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 21);
        slicingWindowOperator.processElement(1, 22);
        slicingWindowOperator.processElement(1, 24);

        //out-of-order tuple
        slicingWindowOperator.processElement(1, 17);

        slicingWindowOperator.processElement(1, 25);


        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 6);
        WindowAssert.assertEquals(resultWindows.get(1),2, 12, 5);
        WindowAssert.assertEquals(resultWindows.get(2),3, 13, 5);
        WindowAssert.assertEquals(resultWindows.get(3),8, 18, 8); //
        WindowAssert.assertEquals(resultWindows.get(4),9, 19, 8); //
        WindowAssert.assertEquals(resultWindows.get(5),10, 20, 8); //
        WindowAssert.assertEquals(resultWindows.get(6),12, 22, 8); //
        WindowAssert.assertEquals(resultWindows.get(7),13, 23, 8);
        WindowAssert.assertEquals(resultWindows.get(8),14, 24, 7);
        WindowAssert.assertEquals(resultWindows.get(9),16, 26, 8);
        WindowAssert.assertEquals(resultWindows.get(10),17, 27, 7);
        WindowAssert.assertEquals(resultWindows.get(11),18, 28, 6); //
        WindowAssert.assertEquals(resultWindows.get(12),19, 29, 5);
        WindowAssert.assertEquals(resultWindows.get(13),21, 31, 4);
        WindowAssert.assertEquals(resultWindows.get(14),22, 32, 3);
        WindowAssert.assertEquals(resultWindows.get(15),24, 34, 2);
        WindowAssert.assertEquals(resultWindows.get(16),25, 35, 1);
    }
}
