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
package de.tub.dima.scotty.slicing.slice;

public abstract class AbstractSlice<InputType, ValueType> implements Slice<InputType, ValueType> {

    private long tStart;
    private long tEnd;

    private Type type;

    private long tLast;
    private long tFirst = Long.MAX_VALUE;

    private long cStart;
    private long cLast;

    public AbstractSlice(long startTs, long endTs, long cStart, long cLast, Type type) {
        this.type = type;
        this.tStart = startTs;
        this.tEnd = endTs;
        this.tLast = startTs;
        this.cLast = cLast;
        this.cStart = cStart;
    }


    @Override
    public void addElement(InputType element, long ts) {
        this.tLast = Math.max(this.tLast, ts);
        this.tFirst = Math.min(this.tFirst, ts);
        this.cLast++;
    }

    @Override
    public void merge(Slice otherSlice) {
        this.tLast = Math.max(this.tLast, otherSlice.getTLast());
        this.tFirst = Math.min(this.tFirst, otherSlice.getTFirst());
        this.setTEnd(Math.max(this.tEnd, otherSlice.getTEnd()));
        this.getAggState().merge(otherSlice.getAggState());
    }



    @Override
    public long getTLast() {
        return tLast;
    }

    @Override
    public long getTFirst() {
        return this.tFirst;
    }

    @Override
    public long getTStart() {
        return tStart;
    }

    @Override
    public void setTStart(long tStart) {
        this.tStart = tStart;
    }

    @Override
    public long getTEnd() {
        return tEnd;
    }

    @Override
    public void setTEnd(long tEnd) {
        this.tEnd = tEnd;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }


    public void setTLast(long tLast) {
        this.tLast = tLast;
    }

    public void setTFirst(long tFirst) {
        this.tFirst = tFirst;
    }

    public void setCStart(long cStart) {
        this.cStart = cStart;
    }

    public void setCLast(long cLast) {
        this.cLast = cLast;
    }

    @Override
    public String toString() {
        return "Slice{" +
                "tStart=" + tStart +
                ", tEnd=" + tEnd +
                ", tLast=" + tLast +
                ", tFirst=" + tFirst +
                ", cFirst=" + cStart +
                ", cLast=" + cLast +
                ", measure=" + type +
                '}';
    }

    public long getCStart() {
        return cStart;
    }

    public long getCLast() {
        return cLast;
    }


}
