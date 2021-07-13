package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.PunctuationWindow;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PunctuationWindowTupleTest {

    private SlicingWindowOperator<Tuple2<Integer, Integer>> slicingWindowOperator;
    private MemoryStateFactory stateFactory;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperator = new SlicingWindowOperator<Tuple2<Integer, Integer>>(stateFactory);
    }
    @Test
    public void inOrderTest() {
        final AggregateFunction<Tuple2<Integer, Integer>, ?, Tuple2<Integer, Integer>> windowFunction = new SumWindowFunctionTest();
        slicingWindowOperator.addWindowFunction(windowFunction);

        Tuple2 punctuation = new Tuple2("key", ".*"); //string matching
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(punctuation));

        slicingWindowOperator.processElement(new Tuple2<>(1,1), 1);
        slicingWindowOperator.processElement(new Tuple2("key",0), 10);
        slicingWindowOperator.processElement(new Tuple2<>(1,2), 19);
        slicingWindowOperator.processElement(new Tuple2<>(1,3), 23);
        slicingWindowOperator.processElement(new Tuple2("key",0), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,4), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,5), 49);
        slicingWindowOperator.processElement(new Tuple2("key",0), 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,new Tuple2<>(1,1));
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,new Tuple2<>(1,5));

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,new Tuple2<>(1,9));
    }

    @Test
    public void inOrderTestRegex() {
        final AggregateFunction<Tuple2<Integer, Integer>, ?, Tuple2<Integer, Integer>> windowFunction = new SumWindowFunctionTest();
        slicingWindowOperator.addWindowFunction(windowFunction);

        Tuple2 punctuation = new Tuple2(".*", 0);
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(punctuation));

        slicingWindowOperator.processElement(new Tuple2<>(1,1), 1);
        slicingWindowOperator.processElement(new Tuple2("123",0), 10);
        slicingWindowOperator.processElement(new Tuple2<>(1,2), 19);
        slicingWindowOperator.processElement(new Tuple2<>(1,3), 23);
        slicingWindowOperator.processElement(new Tuple2("234",0), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,4), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,5), 49);
        slicingWindowOperator.processElement(new Tuple2("345",0), 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,new Tuple2<>(1,1));
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,new Tuple2<>(1,5));

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,new Tuple2<>(1,9));
    }

    @Test
    public void outOfOrderTupleTest() {
        final AggregateFunction<Tuple2<Integer, Integer>, ?, Tuple2<Integer, Integer>> windowFunction = new SumWindowFunctionTest();
        slicingWindowOperator.addWindowFunction(windowFunction);

        Tuple2 p = new Tuple2(1, 0);
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));

        Tuple2<Integer,Integer> t = new Tuple2<>(1,0);
        slicingWindowOperator.processElement(new Tuple2<>(1,1), 1);
        slicingWindowOperator.processElement(t, 10);
        slicingWindowOperator.processElement(new Tuple2<>(1,2), 19);
        slicingWindowOperator.processElement(t, 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,3), 23); //out-of-order tuple
        slicingWindowOperator.processElement(new Tuple2<>(1,5), 49);
        slicingWindowOperator.processElement(t, 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,new Tuple2<>(1,1));
        WindowAssert.assertEquals(resultWindows.get(1), 10,30,new Tuple2<>(1,5));

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 30,51,new Tuple2<>(1,5));
    }

    @Test
    public void twoWindowsTest() {
        final AggregateFunction<Tuple2<Integer, Integer>, ?, Tuple2<Integer, Integer>> windowFunction = new SumWindowFunctionTest();
        slicingWindowOperator.addWindowFunction(windowFunction);

        Tuple2 p = new Tuple2(1, 0);
        Tuple2 p2 = new Tuple2("123", 0);
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p));
        slicingWindowOperator.addWindowAssigner(new PunctuationWindow(p2));

        Tuple2<Integer,Integer> t = new Tuple2<>(1,0);
        slicingWindowOperator.processElement(new Tuple2<>(1,1), 1);
        slicingWindowOperator.processElement(t, 10);
        slicingWindowOperator.processElement(new Tuple2<>(1,2), 19);
        slicingWindowOperator.processElement(new Tuple2("123", 0), 30);
        slicingWindowOperator.processElement(new Tuple2<>(1,3), 23); //out-of-order tuple
        slicingWindowOperator.processElement(new Tuple2<>(1,5), 49);
        slicingWindowOperator.processElement(t, 51);
        slicingWindowOperator.processElement(new Tuple2("123", 0), 51);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(31);
        WindowAssert.assertEquals(resultWindows.get(0), 1,10,new Tuple2<>(1,1));
        WindowAssert.assertEquals(resultWindows.get(1), 1,30,new Tuple2<>(1,6));

        resultWindows = slicingWindowOperator.processWatermark(55);
        WindowAssert.assertEquals(resultWindows.get(0), 10,51,new Tuple2<>(1,10));
        WindowAssert.assertEquals(resultWindows.get(1), 30,51,new Tuple2<>(1,5));
    }

}
