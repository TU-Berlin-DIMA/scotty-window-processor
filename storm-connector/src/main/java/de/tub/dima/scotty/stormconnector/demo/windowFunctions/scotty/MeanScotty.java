package de.tub.dima.scotty.stormconnector.demo.windowFunctions.scotty;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import scala.Tuple2;

public class MeanScotty implements AggregateFunction<Integer, Tuple2<Integer,Integer>,Integer> {

    @Override
    public Tuple2<Integer, Integer> lift(Integer inputTuple) {
        //Sum,Count
        return new Tuple2<>(inputTuple,1);
    }

    @Override
    public Tuple2<Integer, Integer> combine(Tuple2<Integer, Integer> partialAggregate1, Tuple2<Integer, Integer> partialAggregate2) {
        return new Tuple2<>(partialAggregate1._1+partialAggregate2._1,partialAggregate1._2+partialAggregate2._2);
    }

    @Override
    public Integer lower(Tuple2<Integer, Integer> aggregate) {
        return aggregate._1/aggregate._2;
    }
}
