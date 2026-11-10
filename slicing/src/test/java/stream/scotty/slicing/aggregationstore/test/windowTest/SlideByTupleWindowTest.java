/*
 * Copyright The Scotty Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stream.scotty.slicing.aggregationstore.test.windowTest;

import stream.scotty.core.AggregateWindow;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import stream.scotty.core.windowType.SlideByTupleWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.slicing.SlicingWindowOperator;
import stream.scotty.state.memory.MemoryStateFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
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
        slicingWindowOperator.processElement(1, 17); //count=3 if in-order after 16, starts new window at ts 24
        slicingWindowOperator.processElement(1, 18); //shifts window start from 24 to 22
        slicingWindowOperator.processElement(1, 19); //count=5 -> shift window to this position
        slicingWindowOperator.processElement(1, 21); //simple insert into window

        slicingWindowOperator.processElement(1, 25);
        slicingWindowOperator.processElement(1, 27); //starts new window
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
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 5));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 4);
        // same timestamp twice
        slicingWindowOperator.processElement(1, 5);
        slicingWindowOperator.processElement(1, 5); //start of new window
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 12);
        slicingWindowOperator.processElement(1, 13); //start of window
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 16);
        // same timestamp twice
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 19); //start of new window
        slicingWindowOperator.processElement(1, 21);
        slicingWindowOperator.processElement(1, 24);
        slicingWindowOperator.processElement(1, 25); // start of new window after out-of-order tuples arrive
        slicingWindowOperator.processElement(1, 21); // out of order tuple with existing timestamp
        slicingWindowOperator.processElement(1, 22); // out of order tuple
        slicingWindowOperator.processElement(1, 27);
        slicingWindowOperator.processElement(1, 30);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 9);
        WindowAssert.assertEquals(resultWindows.get(1),5, 15, 7);
        WindowAssert.assertEquals(resultWindows.get(2),13, 23, 9);
        WindowAssert.assertEquals(resultWindows.get(3),19, 29, 7);
        WindowAssert.assertEquals(resultWindows.get(4),25, 35, 3);
    }

    @Test
    public void outOfOrderTest6() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 3));

        slicingWindowOperator.processElement(1, 1); //start of window
        slicingWindowOperator.processElement(1, 2);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 4); //start of window
        slicingWindowOperator.processElement(1, 5);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8); //start of window
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 10);
        slicingWindowOperator.processElement(1, 12); //start of window
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 16); //start of window
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 18);
        slicingWindowOperator.processElement(1, 19); //start of window
        slicingWindowOperator.processElement(1, 21);
        slicingWindowOperator.processElement(1, 22);
        slicingWindowOperator.processElement(1, 24); //start of window
        slicingWindowOperator.processElement(1, 25);
        slicingWindowOperator.processElement(1, 27);
        slicingWindowOperator.processElement(1, 30);
        slicingWindowOperator.processElement(1, 31);
        slicingWindowOperator.processElement(1, 32);
        slicingWindowOperator.processElement(1, 34);
        slicingWindowOperator.processElement(1, 35);
        slicingWindowOperator.processElement(1, 37);
        slicingWindowOperator.processElement(1, 38);
        slicingWindowOperator.processElement(1, 39);
        slicingWindowOperator.processElement(1, 40);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(41);

        WindowAssert.assertEquals(resultWindows.get(0),1, 11, 9);
        WindowAssert.assertEquals(resultWindows.get(1),4, 14, 8);
        WindowAssert.assertEquals(resultWindows.get(2),8, 18, 8);
        WindowAssert.assertEquals(resultWindows.get(3),12, 22, 8);
        WindowAssert.assertEquals(resultWindows.get(4),16, 26, 8);
        WindowAssert.assertEquals(resultWindows.get(5),19, 29, 6);
        WindowAssert.assertEquals(resultWindows.get(6),24, 34, 6);
    }

    @Test
    public void outOfOrderTestSlide1() {
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
        WindowAssert.assertEquals(resultWindows.get(3),8, 18, 8);
        WindowAssert.assertEquals(resultWindows.get(4),9, 19, 8);
        WindowAssert.assertEquals(resultWindows.get(5),10, 20, 8);
        WindowAssert.assertEquals(resultWindows.get(6),12, 22, 8);
        WindowAssert.assertEquals(resultWindows.get(7),13, 23, 8);
        WindowAssert.assertEquals(resultWindows.get(8),14, 24, 7);
        WindowAssert.assertEquals(resultWindows.get(9),16, 26, 8);
        WindowAssert.assertEquals(resultWindows.get(10),17, 27, 7);
        WindowAssert.assertEquals(resultWindows.get(11),18, 28, 6);
        WindowAssert.assertEquals(resultWindows.get(12),19, 29, 5);
        WindowAssert.assertEquals(resultWindows.get(13),21, 31, 4);
        WindowAssert.assertEquals(resultWindows.get(14),22, 32, 3);
        WindowAssert.assertEquals(resultWindows.get(15),24, 34, 2);
        WindowAssert.assertEquals(resultWindows.get(16),25, 35, 1);
    }

    @Test
    public void tupleCountContinuesAfterAllWindowsTriggered() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 3));

        slicingWindowOperator.processElement(1, 1); // tuple 0 starts a window
        slicingWindowOperator.processElement(1, 2);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 1, 11, 2);

        slicingWindowOperator.processElement(1, 21); // tuple 2 does not start a window
        slicingWindowOperator.processElement(1, 22); // tuple 3 starts a window
        slicingWindowOperator.processElement(1, 23);
        resultWindows = slicingWindowOperator.processWatermark(40);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 22, 32, 2);
    }

    @Test
    public void watermarkWithoutActiveWindows() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(20);
        WindowAssert.assertEquals(resultWindows.get(0), 1, 11, 2);
        resultWindows = slicingWindowOperator.processWatermark(30);
        Assert.assertTrue(resultWindows.isEmpty());
    }

    @Test
    public void outOfOrderShiftCreatesNextWindow() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        //out-of-order tuple: 8 becomes tuple 4 and has to start a window
        slicingWindowOperator.processElement(1, 2);
        slicingWindowOperator.processElement(1, 20);
        slicingWindowOperator.processElement(1, 21);
        slicingWindowOperator.processElement(1, 22);
        slicingWindowOperator.processElement(1, 23);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        // sorted: 1,2,3,6,8,20,21,22,23 -> windows start at tuples 0,2,4,6,8
        Assert.assertEquals(5, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 1, 11, 5);
        WindowAssert.assertEquals(resultWindows.get(1), 3, 13, 3);
        WindowAssert.assertEquals(resultWindows.get(2), 8, 18, 1);
        WindowAssert.assertEquals(resultWindows.get(3), 21, 31, 3);
        WindowAssert.assertEquals(resultWindows.get(4), 23, 33, 1);
    }

    @Test
    public void outOfOrderBeforeFirstWindow() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));

        slicingWindowOperator.processElement(1, 5);
        slicingWindowOperator.processElement(1, 7);
        slicingWindowOperator.processElement(1, 9);
        slicingWindowOperator.processElement(1, 3);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        Assert.assertEquals(2, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 3, 13, 4);
        WindowAssert.assertEquals(resultWindows.get(1), 7, 17, 2);
    }

    @Test
    public void outOfOrderDuplicateTimestamp() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 6);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        // sorted: 1,3,6,6,8
        Assert.assertEquals(3, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 1, 11, 5);
        WindowAssert.assertEquals(resultWindows.get(1), 6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(2), 8, 18, 1);
    }

    @Test
    public void outOfOrderAfterPartialTrigger() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 17);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(12);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 1, 11, 4);

        slicingWindowOperator.processElement(1, 24);
        slicingWindowOperator.processElement(1, 14);
        slicingWindowOperator.processElement(1, 25);
        resultWindows = slicingWindowOperator.processWatermark(40);

        // sorted: 1,3,6,8,13,14,17,24,25 -> windows start at tuples 2,4,6,8
        Assert.assertEquals(4, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 6, 16, 4);
        WindowAssert.assertEquals(resultWindows.get(1), 13, 23, 3);
        WindowAssert.assertEquals(resultWindows.get(2), 17, 27, 3);
        WindowAssert.assertEquals(resultWindows.get(3), 25, 35, 1);
    }

    @Test
    public void outOfOrderDoesNotCorruptTumblingWindow() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));
        slicingWindowOperator.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 5));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        //out-of-order tuple shifts the start of window [6,16) to 4
        slicingWindowOperator.processElement(1, 4);
        slicingWindowOperator.processElement(1, 17);
        slicingWindowOperator.processElement(1, 24);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(40);

        // tumbling windows
        WindowAssert.assertContains(resultWindows, 0, 5, 3);
        WindowAssert.assertContains(resultWindows, 5, 10, 2);
        WindowAssert.assertContains(resultWindows, 10, 15, 1);
        WindowAssert.assertContains(resultWindows, 15, 20, 1);
        WindowAssert.assertContains(resultWindows, 20, 25, 1);
        // sorted: 1,3,4,6,8,13,17,24 -> slide-by-tuple windows start at tuples 0,2,4,6
        WindowAssert.assertContains(resultWindows, 1, 11, 5);
        WindowAssert.assertContains(resultWindows, 4, 14, 4);
        WindowAssert.assertContains(resultWindows, 8, 18, 3);
        WindowAssert.assertContains(resultWindows, 17, 27, 2);
    }

    @Test
    public void outOfOrderBeforeOldestActiveWindowIsCounted() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(1, 3));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 50);
        slicingWindowOperator.processElement(1, 60);
        slicingWindowOperator.processElement(1, 70);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(5);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 1, 2, 1);

        //not late, but older than the start of the oldest active window [70,71)
        slicingWindowOperator.processElement(1, 55);
        resultWindows = slicingWindowOperator.processWatermark(100);

        // sorted: 1,50,55,60,70 -> windows start at tuples 0,3
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 60, 61, 1);
    }

    @Test
    public void lateTupleBeforeRemovedTimestampsDoesNotShiftWindows() {
        slicingWindowOperator.addWindowFunction((ReduceAggregateFunction<Integer>) (currentAggregate, element) -> currentAggregate + element);
        slicingWindowOperator.addWindowAssigner(new SlideByTupleWindow(10, 2));

        slicingWindowOperator.processElement(1, 1);
        slicingWindowOperator.processElement(1, 3);
        slicingWindowOperator.processElement(1, 6);
        slicingWindowOperator.processElement(1, 8);
        slicingWindowOperator.processElement(1, 13);
        slicingWindowOperator.processElement(1, 17);
        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(12);
        Assert.assertEquals(1, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 1, 11, 4);

        //late tuple older than the oldest active window [6,16), the timestamps 1 and 3 were removed
        slicingWindowOperator.processElement(1, 2);
        slicingWindowOperator.processElement(1, 24);
        slicingWindowOperator.processElement(1, 25);
        resultWindows = slicingWindowOperator.processWatermark(40);

        // the triggered window [1,11) is final, the active windows keep starting at tuples 2,4,6 of 1,3,6,8,13,17,24,25
        Assert.assertEquals(3, resultWindows.size());
        WindowAssert.assertEquals(resultWindows.get(0), 6, 16, 3);
        WindowAssert.assertEquals(resultWindows.get(1), 13, 23, 2);
        WindowAssert.assertEquals(resultWindows.get(2), 24, 34, 2);
    }

    @Test
    public void timestampsArePrunedOnWatermark() throws Exception {
        SlideByTupleWindow.SlideByTupleContext context = new SlideByTupleWindow(10, 3).createContext();
        Field timestamps = SlideByTupleWindow.SlideByTupleContext.class.getDeclaredField("timestamps");
        timestamps.setAccessible(true);

        List<Long> triggeredStarts = new java.util.ArrayList<>();
        for (long ts = 1; ts <= 10000; ts++) {
            context.updateContext(1, ts, new HashSet<>());
            if (ts % 10 == 0) {
                context.triggerWindows((start, end, measure) -> triggeredStarts.add(start), ts - 10, ts);
            }
        }

        // windows start at every third tuple and the buffer only keeps the tuples of the active windows
        Assert.assertEquals(1, (long) triggeredStarts.get(0));
        Assert.assertEquals(9988, (long) triggeredStarts.get(triggeredStarts.size() - 1));
        Assert.assertTrue(((List<?>) timestamps.get(context)).size() <= 20);
    }
}
