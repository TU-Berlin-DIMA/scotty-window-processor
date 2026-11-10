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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class DemoKafkaProducer extends Thread {
    private Properties properties;
    private Producer<Integer, Integer> producer;
    private Random key;
    private Random value;
    private String inputTopicName;

    public DemoKafkaProducer(String inputTopicName) {
        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
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
                producer.send(new ProducerRecord<>(inputTopicName, 1, value.nextInt(10)));
                Thread.sleep(1);
            } catch (InterruptedException e) {
                producer.close();
                interrupt();
            }
        }
    }
}
