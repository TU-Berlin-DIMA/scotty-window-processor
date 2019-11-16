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

package de.tub.dima.scotty.beamconnector.demo.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class Mean implements AggregateFunction<KV<Integer, Integer>, KV<Integer, Pair>, KV<Integer, Integer>>, Serializable {
    @Override
    public KV<Integer, Pair> lift(KV<Integer, Integer> inputTuple) {
        return KV.of(inputTuple.getKey(), new Pair(inputTuple.getValue()));
    }

    @Override
    public KV<Integer, Pair> combine(KV<Integer, Pair> partialAggregate1, KV<Integer, Pair> partialAggregate2) {
        return KV.of(partialAggregate1.getKey(), new Pair(partialAggregate1.getValue().sum + partialAggregate2.getValue().sum,
                partialAggregate1.getValue().count + partialAggregate2.getValue().count));
    }

    @Override
    public KV<Integer, Integer> lower(KV<Integer, Pair> aggregate) {
        return KV.of(aggregate.getKey(), aggregate.getValue().getResult());
    }
}

class Pair {
    int sum;
    int count;

    public Pair(int sum) {
        this.sum = sum;
        this.count = 1;
    }

    public Pair(int sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public int getResult() {
        return sum / count;
    }

    @Override
    public String toString() {
        return getResult() + "";
    }
}