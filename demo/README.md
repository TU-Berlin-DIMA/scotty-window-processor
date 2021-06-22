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

For each of the connectors window aggregation functions such as sum, min, max, quantile are given. 
The window functions have to implement one of the interfaces of the windowFunction folder in the core module.
They depend on the tuples and their attributes. Thus, they have to be adjusted based on the stream.
The user can extend Scotty with window functions beyond these examples as needed.

### Window types

Scotty provides different implementations for window types such as the tumbling window, sliding window, session window 
and fixed-band window.
Scotty enables to add multiple window types to the window operator, so that multiple different concurrent
windows are applies onto the stream.
When assigning these window types, one can choose different window configurations.
It is possible to change the measure (time measure or count measure), the window size, window slide
for sliding windows or session gap for session windows. 
Window types are provided in the core module of Scotty. It is possible to extend Scotty 
with user-defined window types as needed.

### Flink Demo

The Flink subfolder presents three different examples that utilize Scotty as a window operator within the Flink API.
The demo files provide Flink pipelines that are almost identical, but with minor modifications.
This exemplifies how to change the window types and window functions.
The FlinkSumWindowDemo uses a sum aggregation as the window function. This example shows how to
assign three different window types to the window operator. Here, the tumbling window, sliding window and session window are used.
To change this up, the FlinkQuantileDemo applies the quantile window function onto the stream.
A third demo adjusts the tumbling window to be of count-based measure.

Similarly, one can adjust the other demo files to try out different window functions, window types or window configurations.

### Demo Requirements for Samza, Kafka, Spark

* Apache Zookeeper host:localhost , port:2181
* Apache Kafka Server host:localhost , port:9092

#### Demo Usage:
* Start Zookeeper
* Start Kafka Server
* Start SparkSumDemo.java


Note:
There are separate modules for each system within this folder to prevent incompatibilties between the different system versions.
For instance, dependencies on the connectors to Flink and Spark together in one module could cause errors due to different scala versions.