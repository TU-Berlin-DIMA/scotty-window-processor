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
package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.slice.*;
import de.tub.dima.scotty.state.*;

import java.util.*;

public class AggregateWindowState implements AggregateWindow {

    private final long start;
    private final long endTs;
    private final WindowMeasure measure;
    private final AggregateState windowState;

    public AggregateWindowState(long startTs, long endTs, WindowMeasure measure, StateFactory stateFactory, List<AggregateFunction> windowFunctionList) {
        this.start = startTs;
        this.endTs = endTs;
        this.windowState = new AggregateState(stateFactory, windowFunctionList);
        this.measure = measure;
    }

    public boolean containsSlice(Slice currentSlice) {
        if (measure == WindowMeasure.Time) {
            return this.getStart() <= currentSlice.getTStart() && (this.getEnd() > currentSlice.getTLast());
        } else {
            return this.getStart() <= currentSlice.getCStart() && (this.getEnd() >= currentSlice.getCLast());
        }
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return endTs;
    }

    @Override
    public List getAggValues() {
        return windowState.getValues();
    }

    @Override
    public boolean hasValue() {
        return windowState.hasValues();
    }

    public void addState(AggregateState aggregationState) {
        this.windowState.merge(aggregationState);
    }

    public WindowMeasure getMeasure() {
        return measure;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateWindowState that = (AggregateWindowState) o;
        return start == that.start &&
                endTs == that.endTs &&
                Objects.equals(windowState, that.windowState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, endTs, windowState);
    }

    @Override
    public String toString() {
        return "WindowResult(" +
                measure.toString() + ","+
                start + "-" + endTs +
                "," + windowState +
                ')';
    }
}
