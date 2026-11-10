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
package stream.scotty.core;

import stream.scotty.core.windowType.*;

import java.util.*;

/**
 * The result of a triggered window.
 * It holds the final aggregates of the window, which are computed once when the window is triggered,
 * and neither the partial aggregates nor the aggregate functions.
 * @param <T> type of the final aggregates
 */
public class AggregateWindowResult<T> implements AggregateWindow<T> {

    private final WindowMeasure measure;
    private final long start;
    private final long end;
    private final ArrayList<T> aggValues;
    private final boolean hasValue;

    public AggregateWindowResult(WindowMeasure measure, long start, long end, List<T> aggValues, boolean hasValue) {
        this.measure = measure;
        this.start = start;
        this.end = end;
        // a plain ArrayList, so that field based serializers like Kryo can copy the result
        this.aggValues = new ArrayList<>(aggValues);
        this.hasValue = hasValue;
    }

    @Override
    public WindowMeasure getMeasure() {
        return measure;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public List<T> getAggValues() {
        return aggValues;
    }

    @Override
    public boolean hasValue() {
        return hasValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateWindowResult<?> that = (AggregateWindowResult<?>) o;
        return start == that.start &&
                end == that.end &&
                hasValue == that.hasValue &&
                measure == that.measure &&
                Objects.equals(aggValues, that.aggValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(measure, start, end, aggValues, hasValue);
    }

    @Override
    public String toString() {
        return "WindowResult(" +
                measure + "," +
                start + "-" + end +
                "," + aggValues +
                ')';
    }
}
