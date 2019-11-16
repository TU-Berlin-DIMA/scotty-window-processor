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
package de.tub.dima.scotty.stormconnector.demo.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

public class Mean implements AggregateFunction<Integer, Pair, Integer> {

    @Override
    public Pair lift(Integer inputTuple) {
        return new Pair(inputTuple);
    }

    @Override
    public Pair combine(Pair partialAggregate1, Pair partialAggregate2) {
        return new Pair(partialAggregate1.sum + partialAggregate2.sum, partialAggregate1.count + partialAggregate2.count);
    }

    @Override
    public Integer lower(Pair aggregate) {
        return aggregate.getResult();
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
        return getResult()+"";
    }
}
