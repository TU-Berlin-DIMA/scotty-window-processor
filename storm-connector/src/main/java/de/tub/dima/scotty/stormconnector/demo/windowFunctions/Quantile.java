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
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import de.tub.dima.scotty.stormconnector.demo.windowFunctions.QuantileTreeMap;

public class Quantile implements AggregateFunction<Integer, QuantileTreeMap, Integer>, CloneablePartialStateFunction<QuantileTreeMap> {
    private final double quantile;

    public Quantile(double quantile) {
        this.quantile = quantile;
    }

    @Override
    public QuantileTreeMap lift(Integer inputTuple) {
        return new QuantileTreeMap(inputTuple,quantile);
    }

    @Override
    public QuantileTreeMap combine(QuantileTreeMap partialAggregate1, QuantileTreeMap partialAggregate2) {
        return partialAggregate1.merge(partialAggregate2);
    }

    @Override
    public QuantileTreeMap liftAndCombine(QuantileTreeMap partialAggregate, Integer inputTuple) {
        partialAggregate.addValue(inputTuple);
        return partialAggregate;
    }

    @Override
    public Integer lower(QuantileTreeMap aggregate) {
        return aggregate.getQuantile();
    }

    @Override
    public QuantileTreeMap clone(QuantileTreeMap partialAggregate) {
        return partialAggregate.clone();
    }
}
