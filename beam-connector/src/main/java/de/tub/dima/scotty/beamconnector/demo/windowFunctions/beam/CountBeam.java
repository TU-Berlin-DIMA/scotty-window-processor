package de.tub.dima.scotty.beamconnector.demo.windowFunctions.beam;

import org.apache.beam.sdk.transforms.Combine;

public class CountBeam extends Combine.CombineFn<Integer,Integer,Integer> {

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer addInput(Integer accumulator, Integer input) {
        return ++accumulator;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators) {
        int sum = 0;
        for (int l : accumulators)
            sum +=l;
        return sum;
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
        return accumulator;
    }

}