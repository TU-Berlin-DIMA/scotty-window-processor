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
package stream.scotty.demo.kafkaStreams;

import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.kafkastreamsconnector.KeyedScottyWindowOperatorSupplier;
import stream.scotty.core.windowFunction.builtin.Sum;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class KafkaStreamsSumDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "SumDemo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        String INPUT_DESCRIPTOR_NAME = "testInput";

        /**
         * Topology approach
         */

        KeyedScottyWindowOperatorSupplier<Integer, Integer> processorSupplier = new KeyedScottyWindowOperatorSupplier<>(Sum.create(), 100);
        processorSupplier
            .addWindow(new TumblingWindow(WindowMeasure.Time, 2000))
            .addWindow(new SlidingWindow(WindowMeasure.Time, 5000,1000));


        Topology demoTopology = new Topology();
        demoTopology.addSource("TestSource","testInput")
            .addProcessor("ScottyProcess", processorSupplier, "TestSource")
            .addProcessor("ResultPrinter", DemoPrinter::new, "ScottyProcess")
            .addSink("TestSink","testOutput","ResultPrinter");

        System.out.println(demoTopology.describe());
        Thread demoSource = new DemoKafkaProducer(INPUT_DESCRIPTOR_NAME);
        KafkaStreams scottyProcessing = new KafkaStreams(demoTopology,props);
        demoSource.start();
        scottyProcessing.start();

        /**
         * StreamBuilder approach
         */
        /*
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer,Integer> input = builder.stream("testInput");
        KeyedScottyWindowOperatorSupplier<Integer, Integer> processorSupplier = new KeyedScottyWindowOperatorSupplier<>(Sum.create(), 100);
        processorSupplier
            .addWindow(new TumblingWindow(WindowMeasure.Time, 2000))
            .addWindow(new SlidingWindow(WindowMeasure.Time, 5000,1000));
        input.process(processorSupplier);
        Thread demoSource = new DemoKafkaProducer(INPUT_DESCRIPTOR_NAME);
        KafkaStreams streams = new KafkaStreams(builder.build(),props);
        demoSource.start();
        streams.start();
        */
    }
}
