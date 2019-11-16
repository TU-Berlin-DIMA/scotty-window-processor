/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.tub.dima.scotty.flinkconnector.demo;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.flinkconnector.*;
import de.tub.dima.scotty.flinkconnector.demo.windowFunctions.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;

import java.io.*;

public class FlinkQuantileDemo implements Serializable {

    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment sev = StreamExecutionEnvironment.createLocalEnvironment();
        sev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<Integer, Integer>> stream = sev.addSource(new DemoSource());

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> windowOperator =
                new KeyedScottyWindowOperator<>(new QuantileWindowFunction(0.5));

        windowOperator.addWindow(new TumblingWindow(WindowMeasure.Time, 1000));

        stream
                .keyBy(0)
                .process(windowOperator)
                .map(x -> x.getAggValues().get(0).f1)
                .print();

        sev.execute("demo");
    }

}
