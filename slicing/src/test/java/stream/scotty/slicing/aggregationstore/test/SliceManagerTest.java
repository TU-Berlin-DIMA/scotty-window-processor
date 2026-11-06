package stream.scotty.slicing.aggregationstore.test;

import stream.scotty.core.WindowCollector;
import stream.scotty.core.windowFunction.ReduceAggregateFunction;
import stream.scotty.core.windowType.ForwardContextAware;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.core.windowType.windowContext.WindowContext;
import stream.scotty.slicing.SliceManager;
import stream.scotty.slicing.WindowManager;
import stream.scotty.slicing.aggregationstore.AggregationStore;
import stream.scotty.slicing.aggregationstore.LazyAggregateStore;
import stream.scotty.slicing.slice.LazySlice;
import stream.scotty.slicing.slice.Slice;
import stream.scotty.slicing.slice.SliceFactory;
import stream.scotty.slicing.slice.StreamRecord;
import stream.scotty.state.StateFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SliceManagerTest {

    /**
     * This test assesses the implementation of the SliceManager and tests the correct shift of tuples.
     */

    AggregationStore<Integer> aggregationStore;
    StateFactory stateFactory;
    WindowManager windowManager;
    SliceFactory<Integer, Integer> sliceFactory;
    SliceManager sliceManager;

    @Before
    public void setup() {
        aggregationStore = new LazyAggregateStore<>();
        stateFactory = new StateFactoryMock();
        windowManager = new WindowManager(stateFactory, aggregationStore);
        sliceFactory = new SliceFactory<>(windowManager, stateFactory);
        sliceManager = new SliceManager(sliceFactory, aggregationStore, windowManager);
        windowManager.addAggregation(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
                return partialAggregate1 + partialAggregate2;
            }
        });
    }

    /**
     * Shift the end of a slice to a lower timestamp and move tuples to correct slice
     */
    @Test
    public void ShiftLowerModificationTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible()));
        sliceManager.processElement(1,1);
        sliceManager.processElement(1,4);
        sliceManager.processElement(1,8);
        sliceManager.processElement(1,9);

        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible()));
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);

        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible()));
        sliceManager.processElement(1,24);

        // out-of-order tuple
        sliceManager.processElement(1,5); // shift slice start from 10-20 to 5-20 and respectively 0-5 -- move record with ts 8 and 9 to next slice

        // slice 0-5
        assertEquals(0, aggregationStore.getSlice(0).getTStart());
        assertEquals(5, aggregationStore.getSlice(0).getTEnd());
        assertEquals(1, aggregationStore.getSlice(0).getTFirst());
        assertEquals(4, aggregationStore.getSlice(0).getTLast());
        // slice 5-20
        assertEquals(5, aggregationStore.getSlice(1).getTStart());
        assertEquals(20, aggregationStore.getSlice(1).getTEnd());
        assertEquals(5, aggregationStore.getSlice(1).getTFirst());
        assertEquals(19, aggregationStore.getSlice(1).getTLast());

        checkRecords(new int[]{5,8,9,14,19}, ((LazySlice)aggregationStore.getSlice(1)).getRecords().iterator());

    }

    /**
     * Shift the end of a slice to a higher timestamp and move tuples to correct slice
     */
    @Test
    public void ShiftHigherModificationTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible()));
        sliceManager.processElement(1,1);

        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible()));
        sliceManager.processElement(1,12);
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);

        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible()));
        sliceManager.processElement(1,24);

        // out-of-order tuple
        sliceManager.processElement(1,15); // shift slice end from 0-10 to 0-15 and respectively 15-20 -- move tuple with ts 12 and 14 to previous slice

        // slice 0-15
        assertEquals(0, aggregationStore.getSlice(0).getTStart());
        assertEquals(15, aggregationStore.getSlice(0).getTEnd());
        assertEquals(1, aggregationStore.getSlice(0).getTFirst());
        assertEquals(14, aggregationStore.getSlice(0).getTLast());
        // slice 15-20
        assertEquals(15, aggregationStore.getSlice(1).getTStart());
        assertEquals(20, aggregationStore.getSlice(1).getTEnd());
        assertEquals(15, aggregationStore.getSlice(1).getTFirst());
        assertEquals(19, aggregationStore.getSlice(1).getTLast());

        checkRecords(new int[]{1,12,14,15}, ((LazySlice)aggregationStore.getSlice(0)).getRecords().iterator());

    }

    /**
     * Split slice into two slices due to a ShiftModification (Slice not Movable) and move tuples into new slice
     */
    @Test
    public void ShiftModificationSplitTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible(2)));
        Slice.Type sliceType = aggregationStore.getSlice(0).getType();
        assertFalse(sliceType.isMovable());

        sliceManager.processElement(1,1);
        sliceManager.processElement(1,4);
        sliceManager.processElement(1,8);
        sliceManager.processElement(1,9);

        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible(2)));
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);

        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible(2)));
        sliceManager.processElement(1,24);

        // out-of-order tuple
        sliceManager.processElement(1,5); // shift slice start from 10-20 to 5-20 and respectively split slice into 0-5 and 5-10 -- move tuple with ts 8 and 9

        // slice 0-5
        assertEquals(0, aggregationStore.getSlice(0).getTStart());
        assertEquals(5, aggregationStore.getSlice(0).getTEnd());
        assertEquals(1, aggregationStore.getSlice(0).getTFirst());
        assertEquals(4, aggregationStore.getSlice(0).getTLast());
        // added new slice: slice 5-10
        assertEquals(5, aggregationStore.getSlice(1).getTStart());
        assertEquals(10, aggregationStore.getSlice(1).getTEnd());
        assertEquals(5, aggregationStore.getSlice(1).getTFirst());
        assertEquals(9, aggregationStore.getSlice(1).getTLast());
        // slice 10-20
        assertEquals(10, aggregationStore.getSlice(2).getTStart());
        assertEquals(20, aggregationStore.getSlice(2).getTEnd());
        assertEquals(14, aggregationStore.getSlice(2).getTFirst());
        assertEquals(19, aggregationStore.getSlice(2).getTLast());

        checkRecords(new int[]{5,8,9}, ((LazySlice)aggregationStore.getSlice(1)).getRecords().iterator());
    }

    /**
     * Split slice into two slices due to a ShiftModification (Slice is not Movable because Flexible Slice counter != 1) and move tuples into new slice
     */
    @Test
    public void ShiftModificationSplitTest2() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible(2)));
        Slice.Type sliceType = aggregationStore.getSlice(0).getType();
        assertFalse(sliceType.isMovable());

        sliceManager.processElement(1,1);

        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible(2)));
        sliceManager.processElement(1,12);
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,17);
        sliceManager.processElement(1,19);

        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible(2)));
        sliceManager.processElement(1,24);

        // out-of-order tuple
        sliceManager.processElement(1,15); // shift slice end from 0-10 to 0-15 and respectively split slice into 10-15 and 15-20 -- move tuple with ts 17 and 19

        // slice 0-10
        assertEquals(0, aggregationStore.getSlice(0).getTStart());
        assertEquals(10, aggregationStore.getSlice(0).getTEnd());
        assertEquals(1, aggregationStore.getSlice(0).getTFirst());
        assertEquals(1, aggregationStore.getSlice(0).getTLast());
        // added new slice 10-15
        assertEquals(10, aggregationStore.getSlice(1).getTStart());
        assertEquals(15, aggregationStore.getSlice(1).getTEnd());
        assertEquals(12, aggregationStore.getSlice(1).getTFirst());
        assertEquals(14, aggregationStore.getSlice(1).getTLast());
        // slice 15-20
        assertEquals(15, aggregationStore.getSlice(2).getTStart());
        assertEquals(20, aggregationStore.getSlice(2).getTEnd());
        assertEquals(15, aggregationStore.getSlice(2).getTFirst());
        assertEquals(19, aggregationStore.getSlice(2).getTLast());

        checkRecords(new int[]{15,17,19}, ((LazySlice)aggregationStore.getSlice(2)).getRecords().iterator());
    }

    /**
     * Split one slice into two slices due to an AddModification and move tuples into new slice
     */
    @Test
    public void AddModificationSplitTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible()));
        sliceManager.processElement(1,1);

        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible()));
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);

        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible()));
        sliceManager.processElement(1,22);
        sliceManager.processElement(1,24);
        sliceManager.processElement(1,26);
        sliceManager.processElement(1,27);

        // out-of-order tuple
        sliceManager.processElement(1,25); // split slice 20-30 into 20-25 and add new slice 25-30 -- move tuples with ts 26 and 27 to new slice

        // slice 20-25
        assertEquals(20, aggregationStore.getSlice(2).getTStart());
        assertEquals(25, aggregationStore.getSlice(2).getTEnd());
        assertEquals(22, aggregationStore.getSlice(2).getTFirst());
        assertEquals(24, aggregationStore.getSlice(2).getTLast());
        // slice 25-30
        assertEquals(25, aggregationStore.getSlice(3).getTStart());
        assertEquals(30, aggregationStore.getSlice(3).getTEnd());
        assertEquals(25, aggregationStore.getSlice(3).getTFirst());
        assertEquals(27, aggregationStore.getSlice(3).getTLast());

        checkRecords(new int[]{25,26,27,30}, ((LazySlice)aggregationStore.getSlice(3)).getRecords().iterator());
    }

    /**
     * Test DeleteModification: merge slice
     */
    @Test
    public void DeleteModificationTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible()));
        sliceManager.processElement(1,1);
        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible()));
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);
        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible()));
        sliceManager.processElement(1,24);
        aggregationStore.appendSlice(sliceFactory.createSlice(30, 35, new Slice.Flexible()));
        sliceManager.processElement(1,31);
        sliceManager.processElement(1,33);
        aggregationStore.appendSlice(sliceFactory.createSlice(35, 45, new Slice.Flexible()));
        sliceManager.processElement(1,38);

        sliceManager.processElement(1,35); // merge slice 20-30 and 30-35
        // slice 20-35
        assertEquals(20, aggregationStore.getSlice(2).getTStart());
        assertEquals(35, aggregationStore.getSlice(2).getTEnd());
        assertEquals(24, aggregationStore.getSlice(2).getTFirst());
        assertEquals(33, aggregationStore.getSlice(2).getTLast());
        // slice 35-45
        assertEquals(35, aggregationStore.getSlice(3).getTStart());
        assertEquals(45, aggregationStore.getSlice(3).getTEnd());
        assertEquals(35, aggregationStore.getSlice(3).getTFirst());
        assertEquals(38, aggregationStore.getSlice(3).getTLast());

        checkRecords(new int[]{24,31,33}, ((LazySlice)aggregationStore.getSlice(2)).getRecords().iterator());

    }

    public void checkRecords(int[] values, Iterator sliceRecords){
        int i = 0;
        while (sliceRecords.hasNext()) {
            assertEquals("Wrong tuples in slice", ((StreamRecord)sliceRecords.next()).ts, values[i++]);
        }
        assertFalse("More tuples than expected in slice", sliceRecords.hasNext());
    }

    public class TestWindow implements ForwardContextAware {

        WindowMeasure windowMeasure;

        public TestWindow(WindowMeasure windowMeasure) {
            this.windowMeasure = windowMeasure;
        }

        @Override
        public WindowContext createContext() {
            return new TestWindowContext();
        }

        public class TestWindowContext extends WindowContext<Object>{


            @Override
            public ActiveWindow updateContext(Object o, long position) {

                int index = getWindowIndex(position);
                if(index == -1){
                    return addNewWindow(0, position - (position) % 10 , position + 10 - (position) % 10);
                } else if (position % 5 != 0 && position > getWindow(index).getEnd()){
                    return addNewWindow(index+1, position - (position) % 10 , position + 10 - (position) % 10);
                }

                if(position == 5){
                    shiftStart(getWindow(index+1), position);
                } else if(position == 15){
                    shiftStart(getWindow(index), position);
                } else if(position == 25){
                    return addNewWindow(index, position, position + 10 - (position) % 10);
                } else if(position == 35){
                    return this.mergeWithPre(index);
                }

                return null;
            }

            public int getWindowIndex(long position) {
                int i = 0;
                for (; i < numberOfActiveWindows(); i++) {
                    ActiveWindow s = getWindow(i);
                    if (s.getStart() <= position && s.getEnd() > position)
                        return i;
                }
                return i - 1;
            }

            @Override
            public long assignNextWindowStart(long position) {
                return position + 10 - (position) % 10;
            }

            @Override
            public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
                ActiveWindow w = getWindow(0);
                while (w.getEnd() <= currentWatermark) {
                    aggregateWindows.trigger(w.getStart(), w.getEnd() , windowMeasure);
                    removeWindow(0);
                    if (hasActiveWindows())
                        return;
                    w = getWindow(0);
                }
            }
        }

        @Override
        public WindowMeasure getWindowMeasure() {
            return windowMeasure;
        }
    }

}
