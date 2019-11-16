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
    /**
     * This parameter configures the watermark delay.
     */
    private long watermarkDelay = 1000;

    public DemoSource(){}

    public DemoSource(long watermarkDelay){
        this.watermarkDelay = watermarkDelay;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.key = new Random(42);
        this.value = new Random(43);
    }

    public long lastWaterMarkTs = 0;

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
        while (!canceled) {

            ctx.collectWithTimestamp(new Tuple2<>(1, value.nextInt(10)), System.currentTimeMillis());
            if (lastWaterMarkTs + 1000 < System.currentTimeMillis()) {
                long watermark = System.currentTimeMillis() - watermarkDelay;
                ctx.emitWatermark(new Watermark(watermark));
                lastWaterMarkTs = System.currentTimeMillis();
            }
            Thread.sleep(1);
        }
    }

    @Override
    public void cancel() {
        canceled = true;
    }
}