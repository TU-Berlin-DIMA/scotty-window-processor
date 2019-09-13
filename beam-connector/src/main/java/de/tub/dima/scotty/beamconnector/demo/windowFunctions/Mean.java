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