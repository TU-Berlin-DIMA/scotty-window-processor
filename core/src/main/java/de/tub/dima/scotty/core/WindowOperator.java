package de.tub.dima.scotty.core;

import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

import java.io.*;
import java.util.List;

public interface WindowOperator<InputType> extends Serializable {

    /**
     * Process a new element of the stream
     */
    void processElement(InputType element, long ts);

    /**
     * Process a watermark at a specific timestamp
     */
    List<AggregateWindow> processWatermark(long watermarkTs);

    /**
     * Add a window assigner to the window operator.
     */
    void addWindowAssigner(Window window);

    /**
     * Add a aggregation
     * @param windowFunction
     */
    <OutputType> void addAggregation(AggregateFunction<InputType, ?, OutputType> windowFunction);

    /**
     * Set the max lateness for the window operator.
     * LastWatermark - maxLateness is the point in time where slices get garbage collected and no further late elements are processed.
     * @param maxLateness
     */
    void setMaxLateness(long maxLateness);


}
