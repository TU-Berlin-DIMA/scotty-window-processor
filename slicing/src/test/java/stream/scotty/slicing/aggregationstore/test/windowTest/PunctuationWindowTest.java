package stream.scotty.slicing.aggregationstore.test.windowTest;

import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import stream.scotty.core.windowType.PunctuationWindow;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PunctuationWindowTest {

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

        int p = 0; //defines punctuation value
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));

        //processPunctuation method is invoked in the SlicingWindowOperator
        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(0, 10); //Punctuation - new window starts
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 23);
        slicingWindowOperator.processElement(0, 30); //Punctuation
        slicingWindowOperator.processElement(4, 30);
        slicingWindowOperator.processElement(5, 49);
        slicingWindowOperator.processElement(0, 51); //Punctuation
        slicingWindowOperator.processElement(6, 54);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,1);
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,6);

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,9);
    }

    @Test
    public void inOrderTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);

        int p = 0; //defines punctuation value
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));

        //processPunctuation method is invoked in the SlicingWindowOperator
        slicingWindowOperator.processElement(0, 10); //Punctuation - new window starts

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(10);

        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(3, 23);
        slicingWindowOperator.processElement(0, 30); //Punctuation
        slicingWindowOperator.processElement(4, 30);

        resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 10,30,6);

        slicingWindowOperator.processElement(5, 49);
        slicingWindowOperator.processElement(0, 51); //Punctuation

        resultWindows = slicingWindowOperator.processWatermark(51);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,9);
    }

    @Test
    public void simpleInsert() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);

        int p = 0; //defines punctuation value, does not have to be 0
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p ));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(0, 10); //Punctuation
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 19);
        slicingWindowOperator.processElement(1, 21);
        slicingWindowOperator.processElement(1, 23);
        slicingWindowOperator.processElement(1, 24);
        slicingWindowOperator.processElement(1, 25);
        slicingWindowOperator.processElement(0, 30); //Punctuation
        slicingWindowOperator.processElement(5, 49);
        slicingWindowOperator.processElement(5, 45); //out-of-order tuple, insert before tuple 49
        slicingWindowOperator.processElement(0, 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,1);
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,6);
        WindowAssert.assertEquals(resultWindows.get(2), 30,51,10);
    }

    @Test
    public void simpleInsert2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);

        int p = 0; //defines punctuation value
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(0, 10); //Punctuation
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(2, 21);
        slicingWindowOperator.processElement(2, 23);
        slicingWindowOperator.processElement(2, 24);
        slicingWindowOperator.processElement(0, 30); //Punctuation
        slicingWindowOperator.processElement(5, 49);
        slicingWindowOperator.processElement(2,5); //out-of-order tuple, insert into first window
        slicingWindowOperator.processElement(0, 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,3);
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,8);
        WindowAssert.assertEquals(resultWindows.get(2), 30,51,5);
    }

    @Test
    public void outOfOrderTuplesTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);

        int p = 0; //defines punctuation value
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(0, 10); //Punctuation
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(0, 30); //Punctuation
        //out-of-order tuples
        slicingWindowOperator.processElement(3, 21);
        slicingWindowOperator.processElement(3, 23);
        slicingWindowOperator.processElement(3, 24);

        slicingWindowOperator.processElement(5, 49);
        slicingWindowOperator.processElement(0, 51); //Punctuation

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,1);
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,11);

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,5);
    }

    @Test
    public void outOfOrderTest() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);

        int p = 0; //defines punctuation value
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(0, 10); //Punctuation
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(2,20);
        slicingWindowOperator.processElement(3, 23);
        slicingWindowOperator.processElement(4, 32);
        slicingWindowOperator.processElement(1, 34);
        slicingWindowOperator.processElement(1, 40);
        slicingWindowOperator.processElement(0, 30); //out-of-order Punctuation, splits current slice
        slicingWindowOperator.processElement(5, 49);
        slicingWindowOperator.processElement(0, 51); //Punctuation

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,1);
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,7);

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,11);

    }

    @Test
    public void outOfOrderTest2() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);

        int p = 0; //defines punctuation value
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(0, 10); //Punctuation
        slicingWindowOperator.processElement(2, 19);
        slicingWindowOperator.processElement(2,21);
        slicingWindowOperator.processElement(2, 23);
        slicingWindowOperator.processElement(0, 30); //Punctuation
        slicingWindowOperator.processElement(1, 32);
        slicingWindowOperator.processElement(1, 34);
        slicingWindowOperator.processElement(1, 40);
        slicingWindowOperator.processElement(0, 51); //Punctuation
        slicingWindowOperator.processElement(1, 55);
        slicingWindowOperator.processElement(0, 20);//out-of-order punctuation, splits slice of second window

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(56);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,1);
        WindowAssert.assertEquals(resultWindows.get(1), 10,20,2);
        WindowAssert.assertEquals(resultWindows.get(2), 20,30,4);
        WindowAssert.assertEquals(resultWindows.get(3), 30,51,3);
    }

}
