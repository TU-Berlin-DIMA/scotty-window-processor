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
package stream.scotty.demo.flink;

import stream.scotty.core.windowType.*;
import stream.scotty.flinkconnector.*;
import stream.scotty.core.windowFunction.builtin.Sum;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;

import java.io.*;

public class FlinkSumCountWindowDemo implements Serializable {


    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment sev = StreamExecutionEnvironment.createLocalEnvironment();
        sev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sev.setParallelism(1);
        sev.setMaxParallelism(1);

        DataStream<Tuple2<Integer, Integer>> stream = sev.addSource(new DemoSource());

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Integer> processingFunction =
                new KeyedScottyWindowOperator<>(new Sum<>(tuple -> tuple.f1));

        processingFunction.addWindow(new TumblingWindow(WindowMeasure.Count, 1000));

        stream
                .keyBy(0)
                .process(processingFunction)
                .print();

        sev.execute("demo");
    }

}
