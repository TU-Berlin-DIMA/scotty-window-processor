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
