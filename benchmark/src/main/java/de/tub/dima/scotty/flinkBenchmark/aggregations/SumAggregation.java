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
package de.tub.dima.scotty.flinkBenchmark.aggregations;

import de.tub.dima.scotty.core.windowFunction.*;
import org.apache.flink.api.java.tuple.*;

import java.io.*;

public class SumAggregation implements InvertibleReduceAggregateFunction<Tuple4<String, Integer, Long, Long>>, Serializable {

    @Override
    public Tuple4<String, Integer, Long, Long> invert(Tuple4<String, Integer, Long, Long> currentAggregate, Tuple4<String, Integer, Long, Long> toRemove) {
        return new Tuple4<>(currentAggregate.f0, currentAggregate.f1 - toRemove.f1, currentAggregate.f2, currentAggregate.f3);
    }

    @Override
    public Tuple4<String, Integer, Long, Long> combine(Tuple4<String, Integer, Long, Long> partialAggregate1, Tuple4<String, Integer, Long, Long> partialAggregate2) {
        return new Tuple4<>(partialAggregate1.f0, partialAggregate1.f1 + partialAggregate2.f1, partialAggregate1.f2, partialAggregate1.f3);
    }
}