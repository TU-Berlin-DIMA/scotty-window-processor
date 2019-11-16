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

import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.state.StateFactory;


public class SliceFactory<InputType, ValueType> {

    private final WindowManager windowManager;
    private StateFactory stateFactory;

    public SliceFactory(WindowManager windowManager, StateFactory stateFactory) {
        this.windowManager = windowManager;
        this.stateFactory = stateFactory;
    }

    public Slice<InputType, ValueType> createSlice(long startTs, long maxValue, long startCount, long endCount, Slice.Type type) {
        if(!windowManager.hasCountMeasure()){
            return new EagerSlice<>(stateFactory, windowManager, startTs, maxValue, startCount, endCount, type);
        }
        return new LazySlice<>(stateFactory, windowManager, startTs, maxValue, startCount, endCount, type);
    }
    public Slice<InputType, ValueType> createSlice(long startTs, long maxValue, Slice.Type type) {
        return createSlice(startTs, maxValue, windowManager.getCurrentCount(), windowManager.getCurrentCount(), type);
    }


}
