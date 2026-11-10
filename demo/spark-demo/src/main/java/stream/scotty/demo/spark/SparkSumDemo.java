/*
 * Copyright The Scotty Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stream.scotty.demo.spark;

import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.sparkconnector.demo.DemoEvent;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple3;
import stream.scotty.sparkconnector.KeyedScottyWindowOperator;
import stream.scotty.core.windowFunction.builtin.Sum;

import java.sql.Timestamp;

public class SparkSumDemo {

    public static void main(String[] args) throws Exception {
        String INPUT_DESCRIPTOR_NAME = "testInput";
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        //Initialization
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkStructuredSumDemo")
                .config("spark.master", "local")
                .getOrCreate();

        //Convert input binary parameters to non binary
        Dataset<Tuple3<String, String, Timestamp>> srcDemoFrame = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "testInput")
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value as STRING)", "timestamp")
                .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.TIMESTAMP()));

        MapFunction<Tuple3<Integer, Integer, Timestamp>, DemoEvent> streamToEvents =
                (MapFunction<Tuple3<Integer, Integer, Timestamp>, DemoEvent>) value -> new DemoEvent(value._1(), value._2(), value._3().getTime());

        //Convert input to single object containing all parameters
        Dataset<DemoEvent> scottyDF = srcDemoFrame
                .selectExpr("CAST(key AS INTEGER)", "CAST(value AS INTEGER)", "timestamp")
                .as(Encoders.tuple(Encoders.INT(), Encoders.INT(), Encoders.TIMESTAMP()))
                .map(streamToEvents, Encoders.bean(DemoEvent.class));

        KeyedScottyWindowOperator<Integer, Integer> processingFunction2 = new KeyedScottyWindowOperator<>(Sum.create(), 100);
        processingFunction2
                .addWindow(new TumblingWindow(WindowMeasure.Time, 5000))
                .addWindow(new SlidingWindow(WindowMeasure.Time, 5000, 1000));

        //Apply Scotty processing
        Dataset<Integer> scottyWindowDF = scottyDF
                .flatMap(processingFunction2, Encoders.INT());

        //Display streaming schema
        srcDemoFrame.printSchema();
        scottyDF.printSchema();
        scottyWindowDF.printSchema();
        System.out.println("Streaming : "+srcDemoFrame.isStreaming());

        Thread demoSource = new DemoKafkaProducer(INPUT_DESCRIPTOR_NAME);
        demoSource.start();

        //Trigger is set to 999ms so that each batch represents a processing of 1000ms (due to delays)
        StreamingQuery query = scottyWindowDF.writeStream()
                .outputMode("update")
                .format("console")
                .trigger(Trigger.Continuous(999))
                .start();

        query.awaitTermination();
    }
}
