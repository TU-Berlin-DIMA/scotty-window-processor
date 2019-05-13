package de.tub.dima.scotty.slicing;

import de.tub.dima.scotty.core.windowType.windowContext.*;
import de.tub.dima.scotty.slicing.aggregationstore.*;
import de.tub.dima.scotty.slicing.slice.*;

import java.util.*;

public class SliceManager<InputType> {

    private final SliceFactory<InputType, ?> sliceFactory;
    private final AggregationStore<InputType> aggregationStore;
    private final WindowManager windowManager;

    public SliceManager(final SliceFactory sliceFactory, final AggregationStore<InputType> aggregationStore, final WindowManager windowManager) {
        this.sliceFactory = sliceFactory;
        this.aggregationStore = aggregationStore;
        this.windowManager = windowManager;
    }

    /**
     * Append a new slice. This method is called by the {@link StreamSlicer}
     *
     * @param startTs the start timestamp of the new slice or end timestamp of the previous slice
     * @param type    the end measure of the previous slice
     */
    public void appendSlice(final long startTs, final Slice.Type type) {

        // check if we have to adopt the current slice
        if (!aggregationStore.isEmpty()) {
            final Slice currentSlice = aggregationStore.getCurrentSlice();
            currentSlice.setTEnd(startTs);
            currentSlice.setType(type);
        }

        Slice<InputType, ?> slice = this.sliceFactory.createSlice(startTs, Long.MAX_VALUE, windowManager.getCurrentCount(), windowManager.getCurrentCount(), new Slice.Flexible());
        this.aggregationStore.appendSlice(slice);
    }

    /**
     * Process a tuple and insert it in the correct slice.
     * Depending of the WindowMeasure slices have to be adopted under some conditions.
     *
     * @param element the element which is inserted
     * @param ts      the element timestamp
     */
    public void processElement(InputType element, long ts) {

        if (this.aggregationStore.isEmpty()) {
            appendSlice(0, new Slice.Flexible());
        }

        final Slice currentSlice = this.aggregationStore.getCurrentSlice();

        // is element in order?
        if (ts >= currentSlice.getTLast()) {
            // in order
            this.aggregationStore.insertValueToCurrentSlice(element, ts);
            Set<WindowModifications> windowModifications = new HashSet<>();
            for (WindowContext windowContext : this.windowManager.getContextAwareWindows()) {
                windowContext.updateContext(element, ts, windowModifications);
            }

        } else {
            // out of order

            for (WindowContext<InputType> windowContext : this.windowManager.getContextAwareWindows()) {
                Set<WindowModifications> windowModifications = new HashSet<>();
                WindowContext.ActiveWindow assignedWindow = windowContext.updateContext(element, ts, windowModifications);
                checkSliceEdges(windowModifications);
            }

            // updateSlices(windowStarts);

            int indexOfSlice = this.aggregationStore.findSliceIndexByTimestamp(ts);
            this.aggregationStore.insertValueToSlice(indexOfSlice, element, ts);
            if(this.windowManager.hasCountMeasure()){
                // shift count in slices
                for(; indexOfSlice<= this.aggregationStore.size()-2; indexOfSlice++) {
                    LazySlice<InputType, ?> lazySlice = (LazySlice<InputType, ?>) this.aggregationStore.getSlice(indexOfSlice);
                    StreamRecord<InputType> lastElement = lazySlice.dropLastElement();
                    LazySlice<InputType, ?> nextSlice = (LazySlice<InputType, ?>) this.aggregationStore.getSlice(indexOfSlice + 1);
                    nextSlice.prependElement(lastElement);
                }
            }
        }
    }

    private void checkSliceEdges(Set<WindowModifications> windowModifications) {
        for (WindowModifications mod : windowModifications) {
            if (mod instanceof ShiftModification) {
                long pre = ((ShiftModification) mod).pre;
                long post = ((ShiftModification) mod).post;
                int sliceIndex = this.aggregationStore.findSliceByEnd(pre);
                if(sliceIndex==-1)
                    return;
                Slice currentSlice = this.aggregationStore.getSlice(sliceIndex);

                Slice.Type sliceType = currentSlice.getType();

                if(sliceType.isMovable()){
                    // move slice start
                    Slice nextSlice = this.aggregationStore.getSlice(sliceIndex + 1);
                    currentSlice.setTEnd(post);
                    nextSlice.setTStart(post);
                }
                else{
                    if(sliceType instanceof Slice.Flexible)
                        ((Slice.Flexible) sliceType).decrementCount();
                    // split
                    splitSlice(sliceIndex, post);
                }
            }
            if (mod instanceof DeleteModification) {
                long pre = ((DeleteModification) mod).pre;
                int sliceIndex = this.aggregationStore.findSliceByEnd(pre);
                if(sliceIndex >=0){
                    // slice edge still exists
                    Slice currentSlice = this.aggregationStore.getSlice(sliceIndex);
                    Slice.Type sliceType = currentSlice.getType();
                    if(sliceType.isMovable()){
                        this.aggregationStore.mergeSlice(sliceIndex);
                    }else{
                        if(sliceType instanceof Slice.Flexible)
                            ((Slice.Flexible) sliceType).decrementCount();
                    }
                }
            }
            if (mod instanceof AddModification) {
                long newWindowEdge = ((AddModification) mod).post;
                int sliceIndex = this.aggregationStore.findSliceIndexByTimestamp(newWindowEdge);
                Slice slice = this.aggregationStore.getSlice(sliceIndex);
                if(slice.getTStart() != newWindowEdge && slice.getTEnd() != newWindowEdge ){
                    splitSlice(sliceIndex, ((AddModification) mod).post);
                }
            }
        }
    }

    public void splitSlice(int sliceIndex, long timestamp) {
        Slice sliceA = this.aggregationStore.getSlice(sliceIndex);
        // TODO find count for left and right
        Slice sliceB = this.sliceFactory.createSlice(timestamp, sliceA.getTEnd(), sliceA.getCStart(), sliceA.getCLast(), sliceA.getType());
        sliceA.setTEnd(timestamp);
        sliceA.setType(new Slice.Flexible());
        this.aggregationStore.addSlice(sliceIndex + 1, sliceB);
    }
}
