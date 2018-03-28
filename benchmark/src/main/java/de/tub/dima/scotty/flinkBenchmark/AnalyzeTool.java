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
