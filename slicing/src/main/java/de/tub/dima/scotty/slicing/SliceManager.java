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
            if(ts > Math.min(this.windowManager.getMinAllowedTimestamp(), this.aggregationStore.getSlice(0).getTStart())) {

                for (WindowContext<InputType> windowContext : this.windowManager.getContextAwareWindows()) {
                    Set<WindowModifications> windowModifications = new HashSet<>();
                    WindowContext.ActiveWindow assignedWindow = windowContext.updateContext(element, ts, windowModifications);
                    checkSliceEdges(windowModifications);
                }

                // updateSlices(windowStarts);

                int indexOfSlice = this.aggregationStore.findSliceIndexByTimestamp(ts);
                this.aggregationStore.insertValueToSlice(indexOfSlice, element, ts);
                if (this.windowManager.hasCountMeasure()) {
                    // shift count in slices
                    for (; indexOfSlice <= this.aggregationStore.size() - 2; indexOfSlice++) {
                        LazySlice<InputType, ?> lazySlice = (LazySlice<InputType, ?>) this.aggregationStore.getSlice(indexOfSlice);
                        StreamRecord<InputType> lastElement = lazySlice.dropLastElement();
                        LazySlice<InputType, ?> nextSlice = (LazySlice<InputType, ?>) this.aggregationStore.getSlice(indexOfSlice + 1);
                        nextSlice.prependElement(lastElement);
                    }
                }
            }

            // ts smaller than watermark -> tuple in allowed lateness, might change windows that ended before last watermark
            if (ts < this.windowManager.getLastWatermark() && this.windowManager.getResendWindowsInAllowedLateness()){
                this.windowManager.setLastWatermarkToAllowedLateness();
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
                    continue;
                Slice currentSlice = this.aggregationStore.getSlice(sliceIndex);

                Slice.Type sliceType = currentSlice.getType();

                if(sliceType.isMovable()){
                    // move slice start
                    Slice nextSlice = this.aggregationStore.getSlice(sliceIndex + 1);
                    currentSlice.setTEnd(post);
                    nextSlice.setTStart(post);

                    if(post < pre){
                        // move tuples to nextSlice
                        if (currentSlice instanceof LazySlice) {
                            LazySlice<InputType, ?> lazyCurrentSlice = (LazySlice<InputType, ?>)currentSlice;
                            while ((lazyCurrentSlice.getTFirst() < lazyCurrentSlice.getTLast()) && (lazyCurrentSlice.getTLast() >= post)){
                                StreamRecord<InputType> lastElement = lazyCurrentSlice.dropLastElement();
                                ((LazySlice<InputType, ?>)nextSlice).prependElement(lastElement);
                            }
                        }
                    } else {
                        // move tuples to currentSlice
                        if (currentSlice instanceof LazySlice) {
                            LazySlice<InputType, ?> lazyNextSlice = (LazySlice<InputType, ?>)nextSlice;
                            while ((lazyNextSlice.getTFirst() < lazyNextSlice.getTLast()) && (lazyNextSlice.getTFirst() < post)){
                                StreamRecord<InputType> lastElement = lazyNextSlice.dropFirstElement();
                                ((LazySlice<InputType, ?>)currentSlice).prependElement(lastElement);
                            }
                        }
                    }
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
                        Slice nextSlice = this.aggregationStore.getSlice(sliceIndex+1);
                        //move records to new slice
                        if (nextSlice instanceof LazySlice) {
                            while (((LazySlice<InputType, ?>)nextSlice).getCLast() > 0){
                                StreamRecord<InputType> lastElement = ((LazySlice<InputType, ?>)nextSlice).dropLastElement();
                                ((LazySlice<InputType, ?>)currentSlice).prependElement(lastElement);
                            }
                        }
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
                if(sliceIndex != -1) {
                    Slice slice = this.aggregationStore.getSlice(sliceIndex);
                    if (slice.getTStart() != newWindowEdge && slice.getTEnd() != newWindowEdge) {
                        splitSlice(sliceIndex, ((AddModification) mod).post);
                    }
                }else {
                    Slice sliceFirst = this.aggregationStore.getSlice(0);
                    Slice newSlice = this.sliceFactory.createSlice(newWindowEdge, sliceFirst.getTStart(), 0, 0, sliceFirst.getType());
                    this.aggregationStore.addSlice(0, newSlice);
                }
            }
        }
    }

    public void splitSlice(int sliceIndex, long timestamp) {
        Slice sliceA = this.aggregationStore.getSlice(sliceIndex);
        // TODO find count for left and right
        Slice sliceB;
        if(timestamp < sliceA.getTEnd()) {
            sliceB = this.sliceFactory.createSlice(timestamp, sliceA.getTEnd(), sliceA.getCStart(), sliceA.getCLast(), sliceA.getType());
            sliceA.setTEnd(timestamp);
            sliceA.setType(new Slice.Flexible());
            this.aggregationStore.addSlice(sliceIndex + 1, sliceB);
        } else if(sliceIndex + 1 < this.aggregationStore.size()) {
            sliceA = this.aggregationStore.getSlice(sliceIndex + 1);
            sliceB = this.sliceFactory.createSlice(timestamp, sliceA.getTEnd(), sliceA.getCStart(), sliceA.getCLast(), sliceA.getType());
            sliceA.setTEnd(timestamp);
            sliceA.setType(new Slice.Flexible());
            this.aggregationStore.addSlice(sliceIndex + 2, sliceB);
        } else return;

        //move records to new slice
        if (sliceA instanceof LazySlice) {
            while (((LazySlice<InputType, ?>)sliceA).getTLast() >= timestamp){
                StreamRecord<InputType> lastElement = ((LazySlice<InputType, ?>)sliceA).dropLastElement();
                ((LazySlice<InputType, ?>)sliceB).prependElement(lastElement);
            }
        }
    }
}
