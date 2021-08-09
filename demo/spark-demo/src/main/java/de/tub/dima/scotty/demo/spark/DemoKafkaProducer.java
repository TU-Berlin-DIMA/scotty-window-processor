package de.tub.dima.scotty.demo.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class DemoKafkaProducer extends Thread {
    private Properties properties;
    private Producer<String, String> producer;
    private Random key;
    private Random value;
    private String inputTopicName;

    public DemoKafkaProducer(String inputTopicName) {
        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
        this.key = new Random(42);
        this.value = new Random(43);
        this.inputTopicName = inputTopicName;
        setDaemon(true);
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            try {
                producer.send(new ProducerRecord<>(inputTopicName, "1", Integer.toString(value.nextInt(10))));
                Thread.sleep(1);
            } catch (InterruptedException e) {
                producer.close();
                interrupt();
            }
        }
    }
}
