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

import java.util.List;

/**
 * Created by philipp on 5/29/17.
 */
public class BenchmarkConfig {

	public int throughput;
	public long runtime;
	public String name;

	// [Sliding(1,2), Tumbling(1), Session(2)]
	public List<List<String>> windowConfigurations;

	// [Bucket, Naive, Slicing_Lazy, Slicing_Heap]
	public List<String> configurations;
	// [sum, quantil]
	public List<String> aggFunctions;

	public SessionConfig sessionConfig;


	public class SessionConfig {
		int gapCount = 0;
		int minGapTime = 0;
		int maxGapTime = 0;
	}
}
