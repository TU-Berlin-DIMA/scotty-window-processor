package de.tub.dima.scotty.beamconnector.demo;

import de.tub.dima.scotty.beamconnector.KeyedScottyWindowOperator;
import de.tub.dima.scotty.beamconnector.demo.dataGenerator.TimeStampGenerator;
import de.tub.dima.scotty.beamconnector.demo.windowFunctions.scotty.SumScotty;
import de.tub.dima.scotty.beamconnector.demo.dataGenerator.DataGeneratorSource;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;


/*
 * Don't forget to set proper runner arguments
 *
 * For Default Direct Runner
 * --runner=DirectRunner --parallelism=1
 *
 * For Flink Runner
 * --runner=FlinkRunner --parallelism=1 --streaming=true
 *
 * */

public class BeamSumDemo {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);
        System.out.println("Running Pipeline\n " + p.getOptions());

        PCollection<KV<Integer, Integer>> data = p.begin().apply(Read.from(new DataGeneratorSource(0, new TimeStampGenerator())));

        KeyedScottyWindowOperator<Integer, Integer> scottyWindowDoFn = new KeyedScottyWindowOperator<Integer, Integer>(0, new SumScotty());
        scottyWindowDoFn.addWindow(new TumblingWindow(WindowMeasure.Time, 5000));
        scottyWindowDoFn.addWindow(new SlidingWindow(WindowMeasure.Time, 2000, 1000));

        //Apply Scotty Windowing
        PCollection<String> result = data.apply(ParDo.of(scottyWindowDoFn));

        //Print window results
        result.apply(ParDo.of(new printObject()));
        p.run().waitUntilFinish();

    }

    static class printObject extends DoFn<Object, String> {
        @ProcessElement
        public void processElement(@Element Object input) {
            System.out.println(input.toString());
        }
    }
}
