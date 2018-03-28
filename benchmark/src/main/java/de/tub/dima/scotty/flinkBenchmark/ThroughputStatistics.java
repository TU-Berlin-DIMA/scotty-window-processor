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
