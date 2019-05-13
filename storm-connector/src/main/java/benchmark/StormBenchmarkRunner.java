package benchmark;

import benchmark.jobs.ScottyBenchmarkJob;
import benchmark.jobs.StormBenchmarkJob;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.tub.dima.scotty.core.TimeMeasure;
import de.tub.dima.scotty.core.windowType.*;
import org.javatuples.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static de.tub.dima.scotty.core.TimeMeasure.seconds;

/*
 * @author Batuhan Tüter
 * Driver class to start Storm Benchmarks.
 *
 * */

public class StormBenchmarkRunner {

    private static String configPath;

    public static void main(String[] args) throws Exception {

        configPath = args[0];

        BenchmarkConfig config = loadConfig();

        PrintWriter resultWriter = new PrintWriter(new FileOutputStream(new File("result_" + config.name + ".txt"), true));
        List<Pair<Long, Long>> gaps = Collections.emptyList();
        if (config.sessionConfig != null)
            gaps = generateSessionGaps(config.sessionConfig.gapCount, (int) TimeMeasure.minutes(2).toMilliseconds(), config.sessionConfig.minGapTime, config.sessionConfig.maxGapTime);

        System.out.println(gaps);
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

        //At the moment, this implementation does not support multiple windows, configurations and aggregation in one configuration file.
        for (List<String> windows : config.windowConfigurations) {
            for (String benchConfig : config.configurations) {
                for (String agg : config.aggFunctions) {
                    System.out.println("\n\n\n\n\n\n\n");

                    System.out.println("Start Benchmark " + benchConfig + ", throughput " + config.throughput + " with windows " + config.windowConfigurations);
                    System.out.println("\n\n\n\n\n\n\n");
                    switch (benchConfig) {
                        case "Slicing": {
                            new ScottyBenchmarkJob(getAssigners(windows), seconds(config.runtime).toMilliseconds(), config.throughput, gaps);
                            break;
                        }
                        case "Storm": {
                            new StormBenchmarkJob(getAssigners(windows), seconds(config.runtime).toMilliseconds(), config.throughput, gaps);
                            break;
                        }
                    }

                    //System.out.println(ThroughputStatistics.getInstance().toString());
                    resultWriter.append(benchConfig + "\t" + config.throughput + "\t" + windows + " \t" + agg + " \t" +
                            ThroughputStatistics.getInstance().mean() + "\t");
                    resultWriter.append("\n");
                    resultWriter.flush();
                    ThroughputStatistics.getInstance().clean();

                    /* Since the throuhgput mean is written to disk, we can kill a topology by passing topology instances to this class.
                     * But, Storm is not functioning properly after killing a topology if another topology is being executed.
                     *  This can only work if there is no another topology that waits to be submitted after killing the current topology.
                     *  If there is only one topology that needs to be executed in a benchmark, it is safe to kill the topology after the mean is written to disk.
                     *
                     *  So far, we kill and submit new topologies manually.
                     *  In other words, for loops execute only once.
                     *  If you submit multiple windows, configurations or aggregations in your config file, multiple topologies will be submitted and run concurrently,
                     *  which will drastically reduce the performance.
                     *  If you want to submit multiple topologies at the same time, make sure each submitted topology has a different name.
                     */
                }
            }
        }
        resultWriter.close();
    }

    private static List<Window> getAssigners(List<String> config) {

        List<Window> multiQueryAssigner = new ArrayList<Window>();
        for (String windowConfig : config) {
            multiQueryAssigner.addAll(getAssigner(windowConfig));
        }
        System.out.println("Generated Windows: " + multiQueryAssigner.size() + " " + multiQueryAssigner);
        return multiQueryAssigner;
    }

    private static Collection<Window> getAssigner(String windowConfig) {

        // [Sliding(1,2), Tumbling(1), Session(2)]
        Pattern pattern = Pattern.compile("\\d+");
        if (windowConfig.startsWith("Sliding")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int WINDOW_SIZE = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_SLIDE = Integer.valueOf(matcher.group(0));
            return Collections.singleton(new SlidingWindow(WindowMeasure.Time, TimeMeasure.of(WINDOW_SIZE, TimeUnit.MILLISECONDS).toMilliseconds(), TimeMeasure.of(WINDOW_SLIDE, TimeUnit.MILLISECONDS).toMilliseconds()));
        }

        if (windowConfig.startsWith("Tumbling")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int WINDOW_SIZE = Integer.valueOf(matcher.group(0));
            return Collections.singleton(new TumblingWindow(WindowMeasure.Time, TimeMeasure.of(WINDOW_SIZE, TimeUnit.SECONDS).toMilliseconds()));
        }

        if (windowConfig.startsWith("Session")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int GAP_SIZE = Integer.valueOf(matcher.group(0));
            return Collections.singleton(new SessionWindow(WindowMeasure.Time, seconds(GAP_SIZE).toMilliseconds()));
        }

        if (windowConfig.startsWith("RandomSession")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int Session_NR = Integer.valueOf(matcher.group(0));
            matcher.find();
            int MinGap = Integer.valueOf(matcher.group(0));
            matcher.find();
            int MaxGap = Integer.valueOf(matcher.group(0));
            Random random = new Random(10);
            Collection<Window> resultList = new ArrayList<>();
            for (int i = 0; i < Session_NR; i++) {
                long GapSize = MinGap + random.nextInt(MaxGap - MinGap);
                resultList.add(new SessionWindow(WindowMeasure.Time, seconds(GapSize).toMilliseconds()));
            }
            return resultList;
        }
        if (windowConfig.startsWith("randomTumbling")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int WINDOW_NR = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_MIN_SIZE = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_MAX_SIZE = Integer.valueOf(matcher.group(0));
            Random random = new Random(10);
            Collection<Window> resultList = new ArrayList<>();
            for (int i = 0; i < WINDOW_NR; i++) {
                long WINDOW_SIZE = (long) (WINDOW_MIN_SIZE + random.nextDouble() * (WINDOW_MAX_SIZE - WINDOW_MIN_SIZE));
                resultList.add(new TumblingWindow(WindowMeasure.Time, TimeMeasure.of(WINDOW_SIZE, TimeUnit.SECONDS).toMilliseconds()));
            }
            return resultList;
        }

        if (windowConfig.startsWith("randomCount")) {
            Matcher matcher = pattern.matcher(windowConfig);
            matcher.find();
            int WINDOW_NR = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_MIN_SIZE = Integer.valueOf(matcher.group(0));
            matcher.find();
            int WINDOW_MAX_SIZE = Integer.valueOf(matcher.group(0));
            Random random = new Random(10);
            Collection<Window> resultList = new ArrayList<>();
            for (int i = 0; i < WINDOW_NR; i++) {
                double WINDOW_SIZE = WINDOW_MIN_SIZE + random.nextDouble() * (WINDOW_MAX_SIZE - WINDOW_MIN_SIZE);
                //int WINDOW_SIZE = random.nextInt(WINDOW_MAX_SIZE - WINDOW_MIN_SIZE) + WINDOW_MIN_SIZE;

                //resultList.add(CountWindowsAssigner.of((int) WINDOW_SIZE));
            }
            return resultList;
        }
        return null;
    }

    public static List<Pair<Long, Long>> generateSessionGaps(int gapCount, int maxTs, int minGapTime, int maxGapTime) {
        Random gapStartRandom = new Random(333);
        Random gapLengthRandom = new Random(777);

        List<Pair<Long, Long>> result = new ArrayList<>();
        for (int i = 0; i < gapCount; i++) {
            long gapStart = gapStartRandom.nextInt(maxTs);
            long gapLength = gapLengthRandom.nextInt(maxGapTime - minGapTime) + minGapTime;
            result.add(new Pair<>(gapStart, gapLength));
        }

        result.sort(new Comparator<Pair<Long, Long>>() {
            @Override
            public int compare(final Pair<Long, Long> o1, final Pair<Long, Long> o2) {
                return Long.compare(o1.getValue0(), o2.getValue0());
            }
        });
        return result;
    }


    private static BenchmarkConfig loadConfig() throws Exception {
        try (Reader reader = new InputStreamReader(new FileInputStream(configPath), "UTF-8")) {
            Gson gson = new GsonBuilder().create();
            return gson.fromJson(reader, BenchmarkConfig.class);
        }
    }

}
