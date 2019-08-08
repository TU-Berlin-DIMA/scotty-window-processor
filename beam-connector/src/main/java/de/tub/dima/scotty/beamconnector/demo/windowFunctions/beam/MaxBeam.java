package de.tub.dima.scotty.beamconnector.demo.windowFunctions.beam;

import org.apache.beam.sdk.transforms.Combine;

public class MaxBeam extends Combine.CombineFn<Integer,Integer,Integer> {

    @Override
    public Integer createAccumulator() {
        return Integer.MIN_VALUE;
    }

    @Override
    public Integer addInput(Integer accumulator, Integer input) {
        return Math.max(accumulator,input);
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators) {
        int max= Integer.MIN_VALUE;
        for (int l : accumulators)
            max = Math.max(max,l);
        return max;
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
        return accumulator;
    }

}