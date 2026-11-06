package stream.scotty.slicing.aggregationstore.test.windowTest;

import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import stream.scotty.core.windowType.ThresholdFrame;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;
import org.junit.Assert;
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

    @Test
    public void outOfOrderBelowThresholdBeforeFirstFrame() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(2, 2);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(2, 3); //out-of-order, before the first frame, changes nothing

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 16);
    }

    @Test
    public void outOfOrderAboveThresholdBeforeFirstFrameNotAdjacent() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3); //below threshold, separates tuple 2 from the frame
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(2, 8); //end of frame because 2<3
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(5, 2); //out-of-order, tuple 3 lies in between, so the frame start must not shift

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0),4, 8, 16);
    }

    @Test
    public void splitFrameTriggersFirstPart() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(4, 8);
        slicingWindowOperator.processElement(5, 9);
        slicingWindowOperator.processElement(2, 7); //splits the still open frame, first part [4, 7) is complete

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0),4, 7, 16);
    }

    @Test
    public void splitOpenFrameAtItsLastTuple() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(5, 9);
        slicingWindowOperator.processElement(2, 7); //out-of-order, ends the first frame, tuple 9 starts a new open frame
        slicingWindowOperator.processElement(6, 10);
        slicingWindowOperator.processElement(1, 11); //end of second frame

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        Assert.assertEquals(2, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0),4, 7, 16);
        WindowAssert.assertEquals(resultWindows.get(1),9, 11, 11);
    }

    @Test
    public void framesAcrossWatermarks() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));

        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(2, 6); //end of frame
        slicingWindowOperator.processElement(1, 7);
        slicingWindowOperator.processElement(6, 8); //begin of second frame, still open at the watermark
        slicingWindowOperator.processElement(7, 9);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(10);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0),4, 6, 9);

        slicingWindowOperator.processElement(8, 11);
        slicingWindowOperator.processElement(2, 10); //out-of-order, splits the open frame
        slicingWindowOperator.processElement(9, 12);
        slicingWindowOperator.processElement(1, 13); //end of third frame

        resultWindows = slicingWindowOperator.processWatermark(20);
        Assert.assertEquals(2, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0),8, 10, 13);
        WindowAssert.assertEquals(resultWindows.get(1),11, 13, 17);
    }

    @Test
    public void splitFrameWithTumblingWindow() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new ThresholdFrame(3));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 10));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(5, 4); //begin of frame because 5>3
        slicingWindowOperator.processElement(4, 5);
        slicingWindowOperator.processElement(7, 6);
        slicingWindowOperator.processElement(4, 8);
        slicingWindowOperator.processElement(5, 9);
        slicingWindowOperator.processElement(2, 10);//end of frame because 2<3
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(2, 7); //splits frame into 2 frames

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        Assert.assertEquals(4, resultWindows.size());
        WindowAssert.assertContains(resultWindows, 4, 7, 16);
        WindowAssert.assertContains(resultWindows, 8, 10, 9);
        WindowAssert.assertContains(resultWindows, 0, 10, 28);
        WindowAssert.assertContains(resultWindows, 10, 20, 3);
    }
}
