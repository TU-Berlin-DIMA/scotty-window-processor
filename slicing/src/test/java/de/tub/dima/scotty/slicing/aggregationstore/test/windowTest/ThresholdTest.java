package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.ThresholdFrame;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ThresholdTest {

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
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(8, 12); //begin of second frame
        slicingWindowOperator.processElement(4, 13);
        slicingWindowOperator.processElement(1, 14); //end of second frame

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 16);
        WindowAssert.assertEquals(resultWindows.get(1),12, 14, 12);
    }

    @Test
    public void inOrderSizeTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3, 3));

        slicingWindowOperator.processElement(5, 4); //start of frame because 5 > 3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(3, 8); //end of frame because 3 = 3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(2, 11);
        slicingWindowOperator.processElement(8, 12); //no frame because just 1 tuple
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(5, 16); //start of second frame
        slicingWindowOperator.processElement(5,17);
        slicingWindowOperator.processElement(4, 19);
        slicingWindowOperator.processElement(4, 20);
        slicingWindowOperator.processElement(2, 21); //end of second frame


        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(22);
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 16);
        WindowAssert.assertEquals(resultWindows.get(1),16, 21, 18);
    }

    @Test
    public void simpleInsert() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(8, 12); //begin of second frame
        slicingWindowOperator.processElement(4, 13); //end of second frame
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(5, 7); //out-of-order, insert into first frame

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 21);
        WindowAssert.assertEquals(resultWindows.get(1),12, 14, 12);
    }

    @Test
    public void simpleInsert2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(5, 7);
        slicingWindowOperator.processElement(4, 5); //out-of-order, insert into frame
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(8, 12); //begin of second frame
        slicingWindowOperator.processElement(4, 13); //end of second frame
        slicingWindowOperator.processElement(1, 14);


        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 21);
        WindowAssert.assertEquals(resultWindows.get(1),12, 14, 12);
    }

    @Test
    public void noInsertIntoFrame() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 11);
        slicingWindowOperator.processElement(8, 12); //begin of second frame
        slicingWindowOperator.processElement(4, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(2, 10); //does not change anything, no insert into any frame

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 16);
        WindowAssert.assertEquals(resultWindows.get(1),12, 14, 12);
    }

    @Test
    public void NoShiftAtStartOfFrame() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(8, 12); //begin of second frame
        slicingWindowOperator.processElement(4, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(2, 11); //out-of-order, does not change anything

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 16);
        WindowAssert.assertEquals(resultWindows.get(1),12, 14, 12);
    }

    @Test
    public void noShiftSingleFrame() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(2, 12);
        slicingWindowOperator.processElement(3, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(2, 11);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 16);
    }

    @Test
    public void shiftFrameStart() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(8, 12); //begin of second frame
        slicingWindowOperator.processElement(4, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(5, 11); //out-of-order, shift frame start to 11

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 16);
        WindowAssert.assertEquals(resultWindows.get(1),11, 14, 17);
    }

    @Test
    public void shiftFirstFrameStart() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(8, 12); //begin of second frame
        slicingWindowOperator.processElement(4, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(5, 3); //shifts beginning of first frame

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),3, 8, 21);
        WindowAssert.assertEquals(resultWindows.get(1),12, 14, 12);
    }

    @Test
    public void shiftFrameEnd() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(8, 12); //begin of second frame
        slicingWindowOperator.processElement(4, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(2, 7); //shift end of frame from 8 to 7

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 7, 16);
        WindowAssert.assertEquals(resultWindows.get(1),12, 14, 12);
    }

    @Test
    public void splitFrame() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(4, 8);
        slicingWindowOperator.processElement(5, 9);
        slicingWindowOperator.processElement(2, 10);//end of frame because 2<3
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(2, 7); //splits frame into 2 frames

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0),4, 7, 16);
        WindowAssert.assertEquals(resultWindows.get(1),8, 10, 9);
    }
}
