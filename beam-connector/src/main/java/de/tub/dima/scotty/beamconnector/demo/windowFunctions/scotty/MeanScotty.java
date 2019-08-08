package de.tub.dima.scotty.beamconnector.demo.windowFunctions.scotty;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

public class MeanScotty implements AggregateFunction<KV<Integer, Integer>, KV<Integer, KV<Integer, Integer>>, KV<Integer, Integer>>, Serializable {
    @Override
    public KV<Integer, KV<Integer, Integer>> lift(KV<Integer, Integer> inputTuple) {
        //<Key,<Count,Sum>>
        return KV.of(inputTuple.getKey(), KV.of(1, inputTuple.getValue()));
    }

    @Override
    public KV<Integer, KV<Integer, Integer>> combine(KV<Integer, KV<Integer, Integer>> partialAggregate1, KV<Integer, KV<Integer, Integer>> partialAggregate2) {
        return KV.of(partialAggregate1.getKey(), KV.of(partialAggregate1.getValue().getKey() + partialAggregate2.getValue().getKey(), partialAggregate1.getValue().getValue() + partialAggregate2.getValue().getValue()));
    }

    @Override
    public KV<Integer, Integer> lower(KV<Integer, KV<Integer, Integer>> aggregate) {
        //<Key,Sum/Count>
        return KV.of(aggregate.getKey(),aggregate.getValue().getValue()/aggregate.getValue().getKey());
    }
}