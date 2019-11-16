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


import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.state.StateFactory;

public class EagerSlice<InputType, ValueType> extends AbstractSlice<InputType, ValueType> {

    private final AggregateState<InputType> state;

    public EagerSlice(StateFactory stateFactory, WindowManager windowManager, long startTs, long endTs, long startC, long endC, Type type) {
        super(startTs, endTs,startC, endC, type);
        this.state = new AggregateState<InputType>(stateFactory, windowManager.getAggregations(), null);
    }

    @Override
    public AggregateState getAggState() {
        return state;
    }

    @Override
    public void addElement(InputType element, long ts) {
        super.addElement(element, ts);
        state.addElement(element);
    }


}
