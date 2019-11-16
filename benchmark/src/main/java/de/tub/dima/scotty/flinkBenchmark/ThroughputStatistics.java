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

public class ThroughputStatistics {

	private static ThroughputStatistics statistics;
	private boolean pause;

	private ThroughputStatistics() {
	}

	private double counter = 0;
	private double sum = 0;
	public static ThroughputStatistics getInstance() {
		if (statistics == null)
			statistics = new ThroughputStatistics();
		return statistics;
	}


	public void addThrouputResult(double throuputPerS) {
		if (this.pause)
			return;
		counter++;
		sum += throuputPerS;
	}

	public void clean() {
		counter = 0;
		sum = 0;
	}

	public double mean() {
		return sum / counter;
	}

	@Override
	public String toString() {
		return "\nThroughput Mean: " + (sum / counter);
	}

	public void pause(final boolean pause) {
		this.pause = pause;
	}
}
