package de.tub.dima.scotty.beamconnector.demo.windowFunctions.beam;

import de.tub.dima.scotty.beamconnector.demo.windowFunctions.QuantileTreeMap;
import org.apache.beam.sdk.transforms.Combine;

public class QuantileBeam extends Combine.CombineFn<Integer, QuantileTreeMap, Integer> {
    private final double quantile;

    public QuantileBeam(double quantile) {
        this.quantile = quantile;
    }

    @Override
    public QuantileTreeMap createAccumulator() {
        return new QuantileTreeMap(0,quantile);
    }

    @Override
    public QuantileTreeMap addInput(QuantileTreeMap accumulator, Integer input) {
        return accumulator.addValue(Math.toIntExact(input));
    }

    @Override
    public QuantileTreeMap mergeAccumulators(Iterable<QuantileTreeMap> accumulators) {
        QuantileTreeMap merged = new QuantileTreeMap(0,quantile);
        for(QuantileTreeMap accum: accumulators){
            merged.merge(accum);
        }
        return merged;
    }

    @Override
    public Integer extractOutput(QuantileTreeMap accumulator) {
        return new Integer(accumulator.getQuantile());
    }
}