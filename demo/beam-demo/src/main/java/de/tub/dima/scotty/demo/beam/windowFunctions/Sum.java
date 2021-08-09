package de.tub.dima.scotty.demo.beam.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.InvertibleReduceAggregateFunction;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class Sum implements InvertibleReduceAggregateFunction<KV<Integer, Integer>>, Serializable {

    @Override
    public KV<Integer, Integer> invert( KV<Integer, Integer> currentAggregate,  KV<Integer, Integer> toRemove) {
        return KV.of(currentAggregate.getKey(),currentAggregate.getValue()-toRemove.getValue());
    }

    @Override
    public  KV<Integer, Integer> combine( KV<Integer, Integer> partialAggregate1,  KV<Integer, Integer> partialAggregate2) {
        return KV.of(partialAggregate1.getKey(), partialAggregate1.getValue()+ partialAggregate2.getValue());
    }
}