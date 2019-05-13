package de.tub.dima.scotty.beamconnector.demo.windowFunctions;

import de.tub.dima.scotty.core.windowFunction.InvertibleReduceAggregateFunction;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class SumWindowFunction implements InvertibleReduceAggregateFunction<KV<String, Long>>, Serializable {

    @Override
    public KV<String, Long> invert( KV<String, Long> currentAggregate,  KV<String, Long> toRemove) {
        return KV.of(currentAggregate.getKey(),currentAggregate.getValue()-toRemove.getValue());
    }

    @Override
    public  KV<String, Long> combine( KV<String, Long> partialAggregate1,  KV<String, Long> partialAggregate2) {
        return KV.of(partialAggregate1.getKey(), partialAggregate1.getValue()+ partialAggregate2.getValue());
    }
}