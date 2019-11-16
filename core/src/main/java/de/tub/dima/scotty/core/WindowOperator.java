/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
