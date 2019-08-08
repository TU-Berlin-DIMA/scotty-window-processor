package de.tub.dima.scotty.beamconnector.demo.windowFunctions.beam;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;

public class MeanBeam extends Combine.CombineFn<Integer, KV<Integer,Integer>, Integer>{

    @Override
    public KV<Integer,Integer> createAccumulator() {
        //<Count,Sum>
        return KV.of(0,0);
    }

    @Override
    public KV<Integer,Integer> addInput(KV<Integer,Integer> accumulator, Integer input) {
        return KV.of(accumulator.getKey()+1,accumulator.getValue()+input);
    }

    @Override
    public KV<Integer,Integer> mergeAccumulators(Iterable<KV<Integer,Integer>> accumulators) {
        KV<Integer,Integer> merged = createAccumulator();
        int sum =0;
        int count = 0;
        for (KV<Integer,Integer> accum : accumulators) {
            count= accum.getKey();
            sum = accum.getValue();
        }
        return KV.of(count,sum);
    }

    @Override
    public Integer extractOutput(KV<Integer,Integer> accumulator) {
        return accumulator.getValue()/accumulator.getKey();
    }
}