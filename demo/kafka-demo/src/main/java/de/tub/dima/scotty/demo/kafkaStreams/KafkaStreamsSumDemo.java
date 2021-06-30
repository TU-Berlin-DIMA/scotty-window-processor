package de.tub.dima.scotty.demo.kafkaStreams;

import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.kafkastreamsconnector.KeyedScottyWindowOperator;
import de.tub.dima.scotty.kafkastreamsconnector.KeyedScottyWindowOperatorSupplier;
import de.tub.dima.scotty.demo.kafkaStreams.windowFunctions.SumWindowFunction;
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

        KeyedScottyWindowOperatorSupplier<Integer, Integer> processorSupplier = new KeyedScottyWindowOperatorSupplier<>(new SumWindowFunction(), 100);
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
        KeyedScottyWindowOperatorSupplier<Integer, Integer> processorSupplier = new KeyedScottyWindowOperatorSupplier<>(new SumWindowFunction(), 100);
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
