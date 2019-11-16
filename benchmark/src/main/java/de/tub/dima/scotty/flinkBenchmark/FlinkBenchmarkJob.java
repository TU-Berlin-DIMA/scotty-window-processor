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
package de.tub.dima.scotty.flinkBenchmark;

import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.functions.sink.*;
import org.apache.flink.streaming.api.windowing.time.*;
import org.apache.flink.streaming.api.windowing.windows.*;

import java.util.*;

public class FlinkBenchmarkJob {
    public FlinkBenchmarkJob(List<Window> assigners, StreamExecutionEnvironment env, long runtime, int throughput, List<Tuple2<Long, Long>> gaps) {
        Map<String, String> configMap = new HashMap<>();
        ParameterTool parameters = ParameterTool.fromMap(configMap);

        env.getConfig().setGlobalJobParameters(parameters);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.setMaxParallelism(1);

        DataStream<Tuple4<String, Integer, Long, Long>> messageStream = env
                .addSource(new LoadGeneratorSource(runtime, throughput, gaps));

        messageStream.flatMap(new ThroughputLogger<>(200, throughput));

        final SingleOutputStreamOperator<Tuple4<String, Integer, Long, Long>> timestampsAndWatermarks = messageStream
                .assignTimestampsAndWatermarks(new BenchmarkJob.TimestampsAndWatermarks());

        KeyedStream<Tuple4<String, Integer, Long, Long>, Tuple> keyedStream = timestampsAndWatermarks
                .keyBy(0);

        for (Window w : assigners) {

            if (w instanceof TumblingWindow) {
                WindowedStream<Tuple4<String, Integer, Long, Long>, Tuple, TimeWindow> tw = keyedStream.timeWindow(Time.seconds(((TumblingWindow) w).getSize()));

                tw.sum(1).addSink(new SinkFunction() {

                    @Override
                    public void invoke(final Object value) throws Exception {
                        //System.out.println(value);
                    }
                });
            }
            if (w instanceof SlidingWindow) {
                WindowedStream<Tuple4<String, Integer, Long, Long>, Tuple, TimeWindow> tw = keyedStream.timeWindow(Time.milliseconds(((SlidingWindow) w).getSize()), Time.milliseconds(((SlidingWindow) w).getSlide()));

                tw.sum(1).addSink(new SinkFunction() {

                    @Override
                    public void invoke(final Object value) throws Exception {
                        //System.out.println(value);
                    }
                });
            }


        }


        try {
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
