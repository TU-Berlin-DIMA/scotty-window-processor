package de.tub.dima.scotty.slicing.aggregationstore.test;

import de.tub.dima.scotty.core.WindowCollector;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.ForwardContextAware;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.core.windowType.windowContext.WindowContext;
import de.tub.dima.scotty.slicing.SliceManager;
import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.slicing.aggregationstore.AggregationStore;
import de.tub.dima.scotty.slicing.aggregationstore.LazyAggregateStore;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.slice.SliceFactory;
import de.tub.dima.scotty.state.StateFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
     * Shift the end of a slice to a lower timestamp and move records to correct slice
     */
    @Test
    public void ShiftLowerModificationTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible()));
        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible()));
        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible()));

        sliceManager.processElement(1,1);
        sliceManager.processElement(1,4);
        sliceManager.processElement(1,9);
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);
        sliceManager.processElement(1,24);
        sliceManager.processElement(1,26);
        sliceManager.processElement(1,27);
        sliceManager.processElement(1,5); // shift slice start from 10-20 to 5-20 and respectively 0-5 -- move record with ts 9 to next slice

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

    }

    /**
     * Shift the end of a slice to a higher timestamp and move records to correct slice
     */
    @Test
    public void ShiftHigherModificationTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible()));
        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible()));
        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible()));

        sliceManager.processElement(1,1);
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);
        sliceManager.processElement(1,24);
        sliceManager.processElement(1,26);
        sliceManager.processElement(1,27);
        sliceManager.processElement(1,15); // shift slice end from 0-10 to 0-15 and respectively 15-20 -- move tuple with ts 14 to previous slice

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

    }

    /**
     * Split slice into two slices due to a ShiftModification (Slice not Movable) and move tuples into new slice
     */
    @Test
    public void ShiftModificationSplitTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible(2)));
        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible(2)));
        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible(2)));

        Slice.Type sliceType = aggregationStore.getSlice(0).getType();
        assertFalse(sliceType.isMovable());

        sliceManager.processElement(1,1);
        sliceManager.processElement(1,4);
        sliceManager.processElement(1,9);
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);
        sliceManager.processElement(1,24);
        sliceManager.processElement(1,26);
        sliceManager.processElement(1,27);
        sliceManager.processElement(1,5); // shift slice start from 10-20 to 5-20 and respectively split slice into 0-5 and 5-10 -- move tuple with ts 9

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

    }

    /**
     * Split slice into two slices due to a ShiftModification (Slice Movable) and move tuples into new slice
     */
    @Test
    public void ShiftModificationSplitTest2() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible(2)));
        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible(2)));
        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible(2)));

        Slice.Type sliceType = aggregationStore.getSlice(0).getType();
        assertFalse(sliceType.isMovable());

        sliceManager.processElement(1,1);
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);
        sliceManager.processElement(1,24);
        sliceManager.processElement(1,26);
        sliceManager.processElement(1,27);
        sliceManager.processElement(1,15); // shift slice end from 0-10 to 0-15 and respectively split slice into 10-15 and 15-20

        // slice 0-10
        assertEquals(0, aggregationStore.getSlice(0).getTStart());
        assertEquals(10, aggregationStore.getSlice(0).getTEnd());
        assertEquals(1, aggregationStore.getSlice(0).getTFirst());
        assertEquals(1, aggregationStore.getSlice(0).getTLast());
        // added new slice 10-15
        assertEquals(10, aggregationStore.getSlice(1).getTStart());
        assertEquals(15, aggregationStore.getSlice(1).getTEnd());
        assertEquals(14, aggregationStore.getSlice(1).getTFirst());
        assertEquals(14, aggregationStore.getSlice(1).getTLast());
        // slice 15-20
        assertEquals(15, aggregationStore.getSlice(2).getTStart());
        assertEquals(20, aggregationStore.getSlice(2).getTEnd());
        assertEquals(15, aggregationStore.getSlice(2).getTFirst());
        assertEquals(19, aggregationStore.getSlice(2).getTLast());

    }

    /**
     * Split one slice into two slices due to an AddModification and move tuples into new slice
     */
    @Test
    public void AddModificationSplitTest() {
        windowManager.addWindowAssigner(new TestWindow(WindowMeasure.Time));

        aggregationStore.appendSlice(sliceFactory.createSlice(0, 10, new Slice.Flexible()));
        aggregationStore.appendSlice(sliceFactory.createSlice(10, 20, new Slice.Flexible()));
        aggregationStore.appendSlice(sliceFactory.createSlice(20, 30, new Slice.Flexible()));

        sliceManager.processElement(1,1);
        sliceManager.processElement(1,14);
        sliceManager.processElement(1,19);
        sliceManager.processElement(1,22);
        sliceManager.processElement(1,24);
        sliceManager.processElement(1,26);
        sliceManager.processElement(1,27);
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
                }

                if(position == 15){
                    shiftStart(getWindow(index), position);
                }

                if(position == 25){
                    return addNewWindow(index, position, position + 10 - (position) % 10);
                }

                if(position == 35){
                    mergeWithPre(index);
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
