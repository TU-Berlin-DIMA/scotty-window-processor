package de.tub.dima.scotty.kafkastreamsconnector;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;


/**
 * 
 * Use the {@link KeyedScottyWindowOperator} as a transformer step in the Streams DSL.
 * 
 * @author bjoernv
 *
 * @param <Key> key type
 * @param <Value> value type
 * @param <Result> {@link KeyValue} return type (both key and value type can be set arbitrarily)
 */
public class KeyedScottyWindowTransformer<Key, Value, Result> extends KeyedScottyWindowOperator<Key, Value>  implements Transformer<Key, Value, Result>{

  public KeyedScottyWindowTransformer(AggregateFunction<Value, ?, ?> windowFunction, long allowedLateness) {
    super(windowFunction, allowedLateness);
  }
  
  @Override
  public Result transform(Key key, Value value) {
    this.process(key, value);
    return null;
  }
}
