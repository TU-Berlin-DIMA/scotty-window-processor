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

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {

	public static class Result {


		SummaryStatistics throughputs;


		public Result(SummaryStatistics throughputs) {

			this.throughputs = throughputs;

		}
	}

	public static Result analyze(String file, List<String> toIgnore) throws FileNotFoundException {
		Scanner sc = new Scanner(new File(file));

		String l;

		Pattern throughputPattern = Pattern.compile(".*That's ([0-9.]+) elements\\/second\\/core.*");


		SummaryStatistics throughputs = new SummaryStatistics();
		String currentHost = null;


		while (sc.hasNextLine()) {
			l = sc.nextLine();

			// ---------- throughput ---------------
			Matcher tpMatcher = throughputPattern.matcher(l);
			if (tpMatcher.matches()) {
				double eps = Double.valueOf(tpMatcher.group(1));
				throughputs.addValue(eps);
				//	System.out.println("epts = "+eps);

			}
		}

		return new Result(throughputs);
	}

	public static void main(String[] args) throws FileNotFoundException {
		Result r1 = analyze(args[0], null);

		SummaryStatistics throughputs = r1.throughputs;
		// System.out.println("lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs;");
		System.out.println("all-machines;" + throughputs.getMean() + ";" + throughputs.getMax() + ";" + throughputs.getN());


	}
}
