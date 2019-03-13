package de.tub.dima.scotty.flinkconnector.demo;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.functions.source.*;
import org.apache.flink.streaming.api.watermark.*;

import java.io.*;
import java.util.*;

public class DemoSource extends RichSourceFunction<Tuple2<Integer, Integer>> implements Serializable {

        private Random key;
        private Random value;
        private boolean canceled = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.key = new Random(42);
            this.value = new Random(43);
        }

        public long lastWatermark = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (!canceled) {

                ctx.collectWithTimestamp(new Tuple2<>(1, value.nextInt(10)), System.currentTimeMillis());
                if (lastWatermark + 1000 < System.currentTimeMillis()) {
                    ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                    lastWatermark = System.currentTimeMillis();
                }
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }
    }