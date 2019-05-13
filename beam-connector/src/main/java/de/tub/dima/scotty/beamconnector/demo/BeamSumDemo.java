package de.tub.dima.scotty.beamconnector.demo;

import de.tub.dima.scotty.beamconnector.KeyedScottyDoFn;
import de.tub.dima.scotty.beamconnector.demo.windowFunctions.SumWindowFunction;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Random;



/* Don't forget to set proper arguments
 *
 * For Flink Runner
 * --runner=FlinkRunner --parallelism=1 --streaming=true
 *
 * For Direct Runner
 * --runner=DirectRunner --targetParallelism=1
 *
 * */

public class BeamSumDemo {
    private static final Logger LOG = LoggerFactory.getLogger(BeamSumDemo.class);

    public static void main(String[] args) throws Exception {
        int numberOfElementsPerRate = 1;
        int rateInSec = 1;
        int runtimeSeconds = 1;
        int windowsizeInEventTime = 5;

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);
        System.out.println("Running Pipeline\n " + p.getOptions());
        Instant start = Instant.now();

        //Why %10 of the tuples are lost?
        //Why maxReadTime is throttling the TP?
        PCollection<Long> data = p.apply(GenerateSequence.from(1)
                //.withRate(numberOfElementsPerRate, Duration.standardSeconds(rateInSec))
                .withTimestampFn(new generateTimeStamp()));
                //.withMaxReadTime(Duration.standardSeconds(runtimeSeconds)));

        //Print generated data
        //data.apply(ParDo.of(new printObjectWithTimeStamp()));

        //Turn generated values into KV pairs
        PCollection<KV<String, Long>> kvPairs = data.apply(new makeKVPairs());

        KeyedScottyDoFn scottyWindowDoFn = new KeyedScottyDoFn(new SumWindowFunction());
        scottyWindowDoFn.addWindow(new TumblingWindow(WindowMeasure.Time, windowsizeInEventTime * 1000));

        //Apply Scotty Windowing
        PCollection<String> result = kvPairs.apply(ParDo.of(scottyWindowDoFn));

        //Print window results
        result.apply(ParDo.of(new printObject()));

       /* PCollection<KV<String, Long>> windowed = kvPairs.apply(new tumblingWindowWith5Seconds());

        PCollection<KV<String, Long>> summed = windowed.apply(Sum.longsPerKey());
        summed.apply("Log2",ParDo.of(new logTuple()));*/

        p.run();


    }

    static class generateTimeStamp implements SerializableFunction<Long, Instant> {
        Instant eventTime = new Instant(1);
        Instant processingTime;

        @Override
        public Instant apply(Long input) {
            //processingTime = Instant.now();
            eventTime = eventTime.plus(Duration.millis(1));
            return eventTime;
        }
    }

    static class printObject extends DoFn<Object, String> {
        @ProcessElement
        public void processElement(@Element Object input) {
            System.out.println(input);
        }
    }

    static class printObjectWithTimeStamp extends DoFn<Object, Object> {
        @ProcessElement
        public void processElement(@Element Object input, @Timestamp Instant timeStamp) {
            System.out.println("Object: " + input + " TimeStamp: " + timeStamp.getMillis());
        }
    }

    public static class tumblingWindowWith5Seconds extends PTransform<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<KV<String, Long>> input) {
            return input.apply(
                    Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)))
                            .triggering(AfterWatermark.pastEndOfWindow())
                            .withAllowedLateness(Duration.standardSeconds(1))
                            .discardingFiredPanes());
        }
    }

    //Turns Value data into <Key,Value> data
    public static class makeKVPairs extends PTransform<PCollection<Long>, PCollection<KV<String, Long>>> {
        private ArrayList<String> keys;
        private Random rand;

        makeKVPairs() {
            rand = new Random();
            this.keys = new ArrayList<String>();
            keys.add("1");
            //keys.add("2");
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<Long> input) {
            return input.apply(
                    MapElements.into(
                            TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                            .via((val -> KV.of(keys.get(rand.nextInt(keys.size())), val))));
        }
    }
}
