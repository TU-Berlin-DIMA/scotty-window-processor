package de.tub.dima.scotty.demo.beam;

import de.tub.dima.scotty.beamconnector.KeyedScottyWindowOperator;
import de.tub.dima.scotty.demo.beam.dataGenerator.DataGeneratorSource;
import de.tub.dima.scotty.demo.beam.dataGenerator.TimeStampGenerator;
import de.tub.dima.scotty.demo.beam.windowFunctions.Sum;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class BeamSumDemo {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);
        System.out.println("Running Pipeline\n " + p.getOptions());

        PCollection<KV<Integer, Integer>> data = p.begin().apply(Read.from(new DataGeneratorSource(0, new TimeStampGenerator())));

        KeyedScottyWindowOperator<Integer, Integer> scottyWindowDoFn = new KeyedScottyWindowOperator<Integer, Integer>(0, new Sum());
        scottyWindowDoFn.addWindow(new TumblingWindow(WindowMeasure.Time, 5000));
        //scottyWindowDoFn.addWindow(new SlidingWindow(WindowMeasure.Time, 2000, 1000));
        //scottyWindowDoFn.addWindow(new SessionWindow(WindowMeasure.Time, 2000));

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
