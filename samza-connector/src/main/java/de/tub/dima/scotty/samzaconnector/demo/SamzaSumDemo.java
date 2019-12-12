package de.tub.dima.scotty.samzaconnector.demo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import joptsimple.OptionSet;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;

import java.util.List;
import java.util.Map;

public class SamzaSumDemo implements TaskApplication {
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");
    private static final String SYSTEM_DESCRIPTOR_NAME = "testSystem";
    private static final String INPUT_DESCRIPTOR_NAME = "testInput";
    private static final String OUTPUT_DESCRIPTOR_NAME = "testOutput";

    public static void main(String[] args) {
        String[] configArgs = {"--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory"
                , "--config-path=samza-connector/src/main/Properties/config.properties"};
        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(configArgs);
        Config config = cmdLine.loadConfig(options);
        LocalApplicationRunner runner = new LocalApplicationRunner(new SamzaSumDemo(), config);
        runner.run();
        runner.waitForFinish();
    }

    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
        Thread demoSource = new DemoKafkaProducer(INPUT_DESCRIPTOR_NAME);
        demoSource.start();
        KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(SYSTEM_DESCRIPTOR_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);
        KafkaInputDescriptor kid = ksd.getInputDescriptor(INPUT_DESCRIPTOR_NAME, KVSerde.of(new IntegerSerde(), new IntegerSerde()));
        KafkaOutputDescriptor kod = ksd.getOutputDescriptor(OUTPUT_DESCRIPTOR_NAME, KVSerde.of(new IntegerSerde(), new IntegerSerde()));

        appDescriptor
                .withInputStream(kid)
                .withOutputStream(kod);

        appDescriptor.withTaskFactory(new DemoTaskFactory(SYSTEM_DESCRIPTOR_NAME, OUTPUT_DESCRIPTOR_NAME));

    }
}
