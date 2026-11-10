package stream.scotty.core.windowFunction.builtin;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.windowFunction.AggregateFunction;

import java.io.*;
import java.util.AbstractMap;
import java.util.Map;

/**
 * Connectors like Flink, Spark or Storm ship aggregate functions as serialized objects.
 */
public class SerializationTest {

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T object) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(object);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }

    private static <P, F> F aggregate(AggregateFunction<Integer, P, F> function, int... values) {
        P partial = function.lift(values[0]);
        for (int i = 1; i < values.length; i++) {
            partial = function.liftAndCombine(partial, values[i]);
        }
        return function.lower(partial);
    }

    @Test
    public void functionsWithoutExtractor() throws Exception {
        Assert.assertEquals(Integer.valueOf(6), aggregate(roundTrip(Sum.<Integer>create()), 1, 2, 3));
        Assert.assertEquals(Long.valueOf(3), aggregate(roundTrip(new Count<Integer>()), 1, 2, 3));
        Assert.assertEquals(Integer.valueOf(1), aggregate(roundTrip(Min.<Integer>create()), 1, 2, 3));
        Assert.assertEquals(Integer.valueOf(3), aggregate(roundTrip(Max.<Integer>create()), 1, 2, 3));
        Assert.assertEquals(Double.valueOf(2.0), aggregate(roundTrip(Mean.<Integer>create()), 1, 2, 3));
    }

    @Test
    public void functionsWithExtractor() throws Exception {
        Map.Entry<String, Integer> tuple = new AbstractMap.SimpleEntry<>("key", 42);
        Assert.assertEquals(Integer.valueOf(42), roundTrip(new Sum<Map.Entry<String, Integer>, Integer>(t -> t.getValue())).lift(tuple));
        Assert.assertEquals(Integer.valueOf(42), roundTrip(new Min<Map.Entry<String, Integer>, Integer>(Map.Entry::getValue)).lift(tuple));
        Assert.assertEquals(Integer.valueOf(42), roundTrip(new Max<Map.Entry<String, Integer>, Integer>(Map.Entry::getValue)).lift(tuple));
        Assert.assertEquals(new Mean.SumCount(42, 1), roundTrip(new Mean<Map.Entry<String, Integer>>(Map.Entry::getValue)).lift(tuple));
    }

    @SuppressWarnings("unchecked")
    private static <T> T kryoRoundTrip(T object) throws IOException {
        KryoSerializer<T> serializer = new KryoSerializer<>((Class<T>) object.getClass(), new ExecutionConfig());
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(object, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }

    @Test
    public void kryoWithLambdaExtractor() throws Exception {
        // Flink copies emitted window results with Kryo, they reference the aggregate function to lower partial aggregates
        Sum<Map.Entry<String, Integer>, Integer> sum = kryoRoundTrip(new Sum<Map.Entry<String, Integer>, Integer>(Map.Entry::getValue));
        Mean<Map.Entry<String, Integer>> mean = kryoRoundTrip(new Mean<Map.Entry<String, Integer>>(t -> t.getValue()));
        Assert.assertEquals(Integer.valueOf(5), sum.lower(5));
        Assert.assertEquals(Double.valueOf(2.5), mean.lower(new Mean.SumCount(5, 2)));
    }

    @Test
    public void meanPartialAggregate() throws Exception {
        Mean.SumCount partial = new Mean.SumCount(7.5, 3);
        Assert.assertEquals(partial, roundTrip(partial));
    }
}
