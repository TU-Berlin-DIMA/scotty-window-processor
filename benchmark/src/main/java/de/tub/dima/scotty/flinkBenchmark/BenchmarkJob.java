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
import de.tub.dima.scotty.flinkBenchmark.aggregations.SumAggregation;
import de.tub.dima.scotty.flinkconnector.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * Created by philipp on 5/28/17.
 */
public class BenchmarkJob {

	public BenchmarkJob(List<Window> assigner, StreamExecutionEnvironment env, final long runtime,
						final int throughput, final List<Tuple2<Long, Long>> gaps) {


		Map<String, String> configMap = new HashMap<>();
		ParameterTool parameters = ParameterTool.fromMap(configMap);

		env.getConfig().setGlobalJobParameters(parameters);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		env.setMaxParallelism(1);


		KeyedScottyWindowOperator<Tuple, Tuple4<String, Integer, Long, Long>, Tuple4<String, Integer, Long, Long>> windowOperator =
				new KeyedScottyWindowOperator<>(new SumAggregation());

		for(Window w: assigner){
			windowOperator.addWindow(w);
		}


		DataStream<Tuple4<String, Integer, Long, Long>> messageStream = env
			.addSource(new de.tub.dima.scotty.flinkBenchmark.LoadGeneratorSource(runtime, throughput,  gaps));

		messageStream.flatMap(new de.tub.dima.scotty.flinkBenchmark.ThroughputLogger<>(200, throughput));



		final SingleOutputStreamOperator<Tuple4<String, Integer, Long, Long>> timestampsAndWatermarks = messageStream
			.assignTimestampsAndWatermarks(new TimestampsAndWatermarks());



		timestampsAndWatermarks
				.keyBy(0)
				.process(windowOperator)
				.addSink(new SinkFunction() {

					@Override
					public void invoke(final Object value) throws Exception {
						//System.out.println(value);
					}
				});

		try {
			env.execute();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}




	public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple4<String, Integer, Long, Long>> {
		private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
		private long currentMaxTimestamp;
		private long startTime = System.currentTimeMillis();

		@Override
		public long extractTimestamp(final Tuple4<String, Integer, Long, Long> element, final long previousElementTimestamp) {
			long timestamp = element.f3;
			currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
			return timestamp;
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentMaxTimestamp);
		}

	}
}
