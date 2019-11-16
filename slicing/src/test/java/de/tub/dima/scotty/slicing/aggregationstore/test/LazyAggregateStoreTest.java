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
package de.tub.dima.scotty.slicing.aggregationstore.test;


import de.tub.dima.scotty.core.windowFunction.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.slicing.aggregationstore.*;
import de.tub.dima.scotty.slicing.slice.*;
import de.tub.dima.scotty.slicing.state.*;
import de.tub.dima.scotty.state.*;
import static org.junit.Assert.*;
import org.junit.*;

import java.util.*;

public class LazyAggregateStoreTest {

    AggregationStore<Integer> aggregationStore;
    StateFactory stateFactory;
    WindowManager windowManager;
    SliceFactory<Integer, Integer> sliceFactory;

    @Before
    public void setup() {
        aggregationStore = new LazyAggregateStore<>();
        stateFactory = new StateFactoryMock();
        windowManager = new WindowManager(stateFactory, aggregationStore);
        sliceFactory = new SliceFactory<>(windowManager, stateFactory);
        windowManager.addAggregation(new ReduceAggregateFunction<Integer>() {
            @Override
            public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
                return partialAggregate1 + partialAggregate2;
            }
        });
    }

    @Test
    public void getSliceByIndex() {
        ArrayList<Slice<Integer, Integer>> list = new ArrayList<>();
        list.add(sliceFactory.createSlice(0, 10, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(10, 20, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(20, 30, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(40, 50, new Slice.Fixed()));

        // add all slices to aggregationStore
        list.forEach(aggregationStore::appendSlice);


        // tests
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.get(i), aggregationStore.getSlice(i));
        }

        assertEquals(list.get(list.size() - 1), aggregationStore.getCurrentSlice());

    }

    @Test
    public void findSliceByTs() {
        ArrayList<Slice<Integer, Integer>> list = new ArrayList<>();
        list.add(sliceFactory.createSlice(0, 10, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(10, 20, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(20, 30, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(40, 50, new Slice.Fixed()));

        // add all slices to aggregationStore
        list.forEach(aggregationStore::appendSlice);


        // tests
        for (int i = 0; i < list.size(); i++) {
            Slice<Integer, Integer> expectedSlice = list.get(i);
            assertEquals(i, aggregationStore.findSliceIndexByTimestamp(expectedSlice.getTStart()));
            assertEquals(i, aggregationStore.findSliceIndexByTimestamp(expectedSlice.getTEnd() - 1));
            assertEquals(i, aggregationStore.findSliceIndexByTimestamp(expectedSlice.getTStart() + 5));
        }
    }


    @Test
    public void insertValue() {
        ArrayList<Slice<Integer, Integer>> list = new ArrayList<>();
        list.add(sliceFactory.createSlice(0, 10, new Slice.Fixed()));
        list.add(sliceFactory.createSlice(10, 20,  new Slice.Fixed()));
        list.add(sliceFactory.createSlice(20, 30,  new Slice.Fixed()));
        list.add(sliceFactory.createSlice(40, 50,  new Slice.Fixed()));

        // add all slices to aggregationStore
        list.forEach(aggregationStore::appendSlice);

        aggregationStore.insertValueToSlice(1, 1, 14);
        aggregationStore.insertValueToSlice(2, 2, 22);
        aggregationStore.insertValueToCurrentSlice(3, 22);

        assertNull(aggregationStore.getSlice(0).getAggState().getValues().get(0));
        assertEquals(aggregationStore.getSlice(1).getAggState().getValues().get(0), 1);

    }

    @Test
    public void aggregateWindow() {
        ArrayList<Slice<Integer, Integer>> list = new ArrayList<>();
        list.add(sliceFactory.createSlice(0, 10,   new Slice.Fixed()));
        list.add(sliceFactory.createSlice(10, 20,  new Slice.Fixed()));
        list.add(sliceFactory.createSlice(20, 30,  new Slice.Fixed()));
        list.add(sliceFactory.createSlice(30, 40,  new Slice.Fixed()));

        // add all slices to aggregationStore
        list.forEach(aggregationStore::appendSlice);

        aggregationStore.insertValueToSlice(1, 1, 14);
        aggregationStore.insertValueToSlice(2, 2, 22);
        aggregationStore.insertValueToCurrentSlice(3, 33);


        List<AggregateWindowState> window = new ArrayList<>();
        window.add(new AggregateWindowState(10, 40, WindowMeasure.Time, stateFactory, windowManager.getAggregations()));
        window.add(new AggregateWindowState(10, 20, WindowMeasure.Time, stateFactory, windowManager.getAggregations()));


    }

}
