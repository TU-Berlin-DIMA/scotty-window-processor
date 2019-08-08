package de.tub.dima.scotty.beamconnector.demo.windowFunctions.beam;

import org.apache.beam.sdk.transforms.Combine;

public class MinBeam extends Combine.CombineFn<Integer,Integer,Integer> {

    @Override
    public Integer createAccumulator() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Integer addInput(Integer accumulator, Integer input) {
        return Math.min(accumulator,input);
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators) {
        int min = Integer.MAX_VALUE;
        for (int l : accumulators)
            min = Math.min(min,l);
        return min;
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
        return accumulator;
    }

}