/*
 * Copyright The Scotty Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stream.scotty.demo.beam;

import stream.scotty.beamconnector.KeyedScottyWindowOperator;
import stream.scotty.demo.beam.dataGenerator.DataGeneratorSource;
import stream.scotty.demo.beam.dataGenerator.TimeStampGenerator;
import stream.scotty.core.windowFunction.builtin.Sum;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
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

        KeyedScottyWindowOperator<Integer, Integer> scottyWindowDoFn = new KeyedScottyWindowOperator<Integer, Integer>(0, new Sum<>((KV<Integer, Integer> tuple) -> tuple.getValue()));
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
