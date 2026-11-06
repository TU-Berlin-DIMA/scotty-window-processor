package stream.scotty.demo.kafkaStreams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class DemoPrinter<Key,Value> implements Processor<Key, Value> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(Key key, Value value) {
        System.out.println("Processing result: Key: "+key+" Value: "+value);

        context.forward(key,value);
    }

    @Override
    public void close() {

    }
}
