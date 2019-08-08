package de.tub.dima.scotty.beamconnector.demo.windowFunctions.scotty;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class MinScotty implements ReduceAggregateFunction<KV<Integer, Integer>>, Serializable {

    @Override
    public KV<Integer, Integer> combine(KV<Integer, Integer> partialAggregate1, KV<Integer, Integer> partialAggregate2) {
        return KV.of(partialAggregate1.getKey(), Math.min(partialAggregate1.getValue(), partialAggregate2.getValue()));
    }
}