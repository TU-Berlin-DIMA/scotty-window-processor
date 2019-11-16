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
package de.tub.dima.scotty.flinkconnector.demo.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.*;
import org.apache.flink.api.java.tuple.*;

import java.io.*;

public class SumWindowFunction implements InvertibleReduceAggregateFunction<Tuple2<Integer, Integer>>, Serializable {

    @Override
    public Tuple2<Integer, Integer> invert(Tuple2<Integer, Integer> currentAggregate, Tuple2<Integer, Integer> toRemove) {
        return new Tuple2<>(currentAggregate.f0, currentAggregate.f1 - toRemove.f1);
    }

    @Override
    public Tuple2<Integer, Integer> combine(Tuple2<Integer, Integer> partialAggregate1, Tuple2<Integer, Integer> partialAggregate2) {
        return new Tuple2<>(partialAggregate1.f0, partialAggregate1.f1 + partialAggregate2.f1);
    }
}