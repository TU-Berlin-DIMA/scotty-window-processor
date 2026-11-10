# Demo

This folder contains examples for the connectors to the different stream processing systems 
that are currently provided in the Scotty open source project.
- [Apache Flink](https://flink.apache.org/)
- [Apache Storm](https://storm.apache.org/)
- [Apache Beam](https://beam.apache.org/)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [Apache Samza](http://samza.apache.org/)

The connectors that can be found in the different modules implement the interface of the WindowOperator and depend on the stream processing system. They are
essential to use Scotty as a window aggregator within the API of the respective system.

Each subfolder within this directory provides the resources necessary to run an example of a stream processing pipeline 
on one of the stream processing systems with Scotty as a window operator.

The subfolders provide the following components:

### Demo files

The demo files contain the stream processing pipelines that use Scotty as a window operator within the 
stream processing system.

### Demo Source

Each subfolder provides an implementation for a demo source that continuously generates tuples with arbitrary values
to resemble a data stream. In these demos the tuples consist of two attributes, a key and a value. 
However, the records could have multiple attributes of different data types.

### Window Functions

The core module provides built-in window aggregation functions for all connectors: sum, count, min, max, mean and quantile
(package `stream.scotty.core.windowFunction.builtin`). They either aggregate the tuples themselves or a value
that a `ValueExtractor` takes from each tuple, e.g. `new Sum<>(tuple -> tuple.f1)` for a Flink `Tuple2`.
The demos use these built-in functions.
User-defined window functions have to implement one of the interfaces of the windowFunction folder in the core module.

### Window types

Scotty provides different implementations for window types such as the tumbling window, sliding window, session window,
fixed-band window, punctuation window, slide-by-tuple window and threshold frame.
Scotty enables to add multiple window types to the window operator, so that multiple different concurrent
windows are applies onto the stream.
When assigning these window types, one can choose different window configurations.
It is possible to change the measure (time measure or count measure), the window size, window slide
for sliding windows or session gap for session windows. 
Window types are provided in the core module of Scotty. It is possible to extend Scotty 
with user-defined window types as needed.

### Flink Demo

The Flink subfolder presents four different examples that utilize Scotty as a window operator within the Flink API.
The demo files provide Flink pipelines that are almost identical, but with minor modifications.
This exemplifies how to change the window types and window functions.
- The FlinkSumDemo uses a sum aggregation as the window function. This example shows how to
assign multiple window types to the window operator, here a tumbling window and a sliding window.
- The FlinkQuantileDemo computes the median of each window with the quantile window function.
- The FlinkSumCountWindowDemo adjusts the tumbling window to be of count-based measure.
- The FlinkPunctuationDemo uses punctuation windows: a new window starts at every tuple that matches the punctuation.

Similarly, one can adjust the other demo files to try out different window functions, window types or window configurations.

### Running the Demos

The Flink, Storm and Beam demos run locally without other systems.
Start their main class, e.g. `FlinkSumDemo`, `ScottyDemoTopology` (Storm) or `BeamSumDemo`.

The Kafka Streams, Spark and Samza demos read from the Kafka topic `testInput` on a Kafka broker at `localhost:9092`.
Each demo writes its own demo data to this topic with a `DemoKafkaProducer`.
The Samza demo also configures Apache ZooKeeper at `localhost:2181`.

1. Start a Kafka broker, e.g. with Docker:
   `docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.2`
2. Create the topic:
   `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic testInput`
3. Start the main class of the demo: `KafkaStreamsSumDemo`, `SparkSumDemo` or `SamzaSumDemo`.
   Start the Samza demo from the root folder of the repository, as it reads `samza-connector/src/main/Properties/config.properties`.

The demos write different data formats to the topic (e.g. strings in the Spark demo, integers in the Kafka Streams demo).
Delete the topic and create it again before you start another demo:
`docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic testInput`


Note:
There are separate modules for each system within this folder to prevent incompatibilties between the different system versions.
For instance, dependencies on the connectors to Flink and Spark together in one module could cause errors due to different scala versions.