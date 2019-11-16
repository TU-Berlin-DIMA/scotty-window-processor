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

import de.tub.dima.scotty.beamconnector.demo.windowFunctions.QuantileTreeMap;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class Quantile implements AggregateFunction<
        KV<Integer, Integer>,
        KV<Integer, QuantileTreeMap>,
        KV<Integer,Integer>>,
        CloneablePartialStateFunction<KV<Integer,QuantileTreeMap>>, Serializable {
    private final double quantile;

    public Quantile(double quantile) {
        this.quantile = quantile;
    }

    @Override
    public KV<Integer, Integer> lower(KV<Integer, QuantileTreeMap> aggregate) {
        return KV.of(aggregate.getKey(),aggregate.getValue().getQuantile());
    }

    @Override
    public KV<Integer, QuantileTreeMap> lift(KV<Integer, Integer> inputTuple) {
        return KV.of(inputTuple.getKey(),new QuantileTreeMap(Math.toIntExact(inputTuple.getValue()),quantile));
    }

    @Override
    public KV<Integer, QuantileTreeMap> combine(KV<Integer, QuantileTreeMap> partialAggregate1, KV<Integer, QuantileTreeMap> partialAggregate2) {
        return KV.of(partialAggregate1.getKey(),partialAggregate1.getValue().merge(partialAggregate2.getValue()));
    }

    @Override
    public KV<Integer, QuantileTreeMap> liftAndCombine(KV<Integer, QuantileTreeMap> partialAggregate, KV<Integer, Integer> inputTuple) {
        partialAggregate.getValue().addValue(Math.toIntExact(inputTuple.getValue()));
        return partialAggregate;
    }

    @Override
    public KV<Integer, QuantileTreeMap> clone(KV<Integer, QuantileTreeMap> partialAggregate) {
        return KV.of(partialAggregate.getKey(),partialAggregate.getValue().clone());
    }
}