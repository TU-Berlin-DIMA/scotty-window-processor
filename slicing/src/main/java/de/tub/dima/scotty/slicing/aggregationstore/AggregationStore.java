package de.tub.dima.scotty.slicing.aggregationstore;

import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.state.*;

public interface AggregationStore<InputType> {

    /**
     * @return get the newest slice.
     */
    Slice<InputType, ?> getCurrentSlice();

    /**
     * Lookup for a slice which contains this timestamp.
     * The timestamp is >= slice start < slice end.
     *
     * @param ts element timestamp
     * @return index of containing slice.
     */
    int findSliceIndexByTimestamp(long ts);

    int findSliceIndexByCount(long count);

    /**
     * Returns slice for a given index or @{@link IndexOutOfBoundsException}
     *
     * @param index >= 0 < size
     * @return @Slice
     */
    Slice<InputType, ?> getSlice(int index);

    /**
     * Insert the element to the current slice.
     *
     * @param element the element
     * @param ts      timestamp of record
     */
    void insertValueToCurrentSlice(InputType element, long ts);

    void insertValueToSlice(int index, InputType element, long ts);

    /**
     * Appends a new slice as new current slice
     *
     * @param newSlice
     */
    void appendSlice(Slice<InputType, ?> newSlice);

    int size();

    /**
     * @return true if AggregationStore contains no slices
     */
    boolean isEmpty();

    /**
     * Generates the window aggregates.
     * On every @{@link AggregateWindowState} the aggregated value is set.
     * @param aggregateWindows definition of the requested window
     * @param minTs startTimestamp of the earliest window.
     * @param maxTs endTimestamp of the latest window
     * @param minCount
     * @param maxCount
     */
    void aggregate(WindowManager.AggregationWindowCollector aggregateWindows, long minTs, long maxTs, long minCount, long maxCount);

    /**
     * Add a new Slice at a specific index
     * @param index
     * @param newSlice
     */
    void addSlice(int index, Slice newSlice);

    /**
     * Merging two slices A and B happens in three steps:
     * 1. Update the end of A such that t end (A) ← t end (B).
     * 2. Update the aggregate of A such that a ← a ⊕ b.
     * 3. Delete slice B, which is now merged into A.
     * @param sliceIndex index of slice A
     */
    void mergeSlice(int sliceIndex);

    int findSliceByEnd(long pre);
}
