package stream.scotty.flinkBenchmark;

import com.google.gson.*;
import stream.scotty.core.*;
import stream.scotty.core.windowType.*;
import stream.scotty.core.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.environment.*;
import static org.apache.flink.streaming.api.windowing.time.Time.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

/**
 * Created by philipp on 5/28/17.
 */
public class BenchmarkRunner {

    private static String configPath;

    public static void main(String[] args) throws Exception {

        configPath = args[0];

        BenchmarkConfig config = loadConfig();

        PrintWriter resultWriter = new PrintWriter(new FileOutputStream(new File("result_" + config.name + ".txt"),true));

        Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        List<Tuple2<Long, Long>> gaps = Collections.emptyList();
        if (config.sessionConfig != null)
            gaps = generateSessionGaps(config.sessionConfig.gapCount, (int) TimeMeasure.minutes(2).toMilliseconds(), config.sessionConfig.minGapTime, config.sessionConfig.maxGapTime);

        System.out.println(gaps);
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);


        for (List<String> windows : config.windowConfigurations) {
            for (String benchConfig : config.configurations) {
                for (String agg : config.aggFunctions) {
                    System.out.println("\n\n\n\n\n\n\n");

                    System.out.println("Start Benchmark " + benchConfig + " with windows " + config.windowConfigurations );
                    System.out.println("\n\n\n\n\n\n\n");
                    // [Bucket, Naive, Slicing_Lazy, Slicing_Heap]
                    switch (benchConfig) {
                        case "Slicing": {
                            new BenchmarkJob(getAssigners(windows), env, TimeMeasure.seconds(config.runtime).toMilliseconds(), config.throughput, gaps);
                            break;
                        }
                        case "Flink": {
                            new FlinkBenchmarkJob(getAssigners(windows), env, TimeMeasure.seconds(config.runtime).toMilliseconds(), config.throughput, gaps);
                            break;
                        }
                    }

                    System.out.println(ThroughputStatistics.getInstance().toString());


                    resultWriter.append(benchConfig  + "\t" + windows + " \t" + agg + " \t" +
                           ThroughputStatistics.getInstance().mean() + "\t");
                    resultWriter.append("\n");
                    resultWriter.flush();
                    ThroughputStatistics.getInstance().clean();

                    Thread.sleep(seconds(10).toMilliseconds());
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
            return Collections.singleton(new SessionWindow(WindowMeasure.Time, TimeMeasure.seconds(GAP_SIZE).toMilliseconds()));
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
                resultList.add(new SessionWindow(WindowMeasure.Time, TimeMeasure.seconds(GapSize).toMilliseconds()));
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
                double WINDOW_SIZE = (WINDOW_MIN_SIZE + random.nextDouble() * (WINDOW_MAX_SIZE - WINDOW_MIN_SIZE));
                resultList.add(new TumblingWindow(WindowMeasure.Time, TimeMeasure.of((long)(WINDOW_SIZE*1000), TimeUnit.MILLISECONDS).toMilliseconds()));
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

                resultList.add(new TumblingWindow(WindowMeasure.Count,(int) WINDOW_SIZE));
            }
            return resultList;
        }
        return null;
    }

    public static List<Tuple2<Long, Long>> generateSessionGaps(int gapCount, int maxTs, int minGapTime, int maxGapTime) {
        Random gapStartRandom = new Random(333);
        Random gapLengthRandom = new Random(777);

        List<Tuple2<Long, Long>> result = new ArrayList<>();
        for (int i = 0; i < gapCount; i++) {
            long gapStart = gapStartRandom.nextInt(maxTs);
            long gapLength = gapLengthRandom.nextInt(maxGapTime - minGapTime) + minGapTime;
            result.add(new Tuple2<>(gapStart, gapLength));
        }

        result.sort(new Comparator<Tuple2<Long, Long>>() {
            @Override
            public int compare(final Tuple2<Long, Long> o1, final Tuple2<Long, Long> o2) {
                return Long.compare(o1.f0, o2.f0);
            }
        });
        return result;
    }


    private static BenchmarkConfig loadConfig() throws Exception {
        try (java.io.Reader reader = new InputStreamReader(new FileInputStream(configPath), "UTF-8")) {
            Gson gson = new GsonBuilder().create();
            return gson.fromJson(reader, BenchmarkConfig.class);
        }
    }

}
