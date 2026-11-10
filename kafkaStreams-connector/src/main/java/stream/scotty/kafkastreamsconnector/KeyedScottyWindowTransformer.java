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
package stream.scotty.kafkastreamsconnector;

import stream.scotty.core.windowFunction.AggregateFunction;
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
