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
