/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tub.dima.scotty.beamconnector.demo;

import de.tub.dima.scotty.beamconnector.KeyedScottyWindowOperator;
import de.tub.dima.scotty.beamconnector.demo.dataGenerator.DataGeneratorSource;
import de.tub.dima.scotty.beamconnector.demo.dataGenerator.TimeStampGenerator;
import de.tub.dima.scotty.beamconnector.demo.windowFunctions.Mean;
import de.tub.dima.scotty.beamconnector.demo.windowFunctions.Sum;
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
