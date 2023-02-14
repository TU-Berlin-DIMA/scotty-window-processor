# Scotty: Efficient Window Aggregation for out-of-order Stream Processing [![Build Status](https://travis-ci.org/TU-Berlin-DIMA/scotty-window-processor.svg?branch=master)](https://travis-ci.org/TU-Berlin-DIMA/scotty-window-processor)

This repository provides Scotty, a framework for efficient window aggregations for out-of-order Stream Processing.

### Features:
- High performance window aggregation with stream slicing. 
- Scales to thousands of concurrent windows. 
- Support for Tumbling, Sliding, and Session Windows.
- Initial support for Count-based windows.
- Out-of-order processing.
- Aggregate Sharing among all concurrent windows.
- Connector for [Apache Flink](https://flink.apache.org/).
- Connector for [Apache Storm](https://storm.apache.org/).
- Connector for [Apache Beam](https://beam.apache.org/).
- Connector for [Apache Kafka](https://kafka.apache.org/).
- Connector for [Apache Spark](https://spark.apache.org/).

### Resources:
 - [Paper: Efficient Window Aggregation with General Stream Slicing](http://www.user.tu-berlin.de/powibol/assets/publications/traub-efficient-window-aggregation-with-general-stream-slicing-edbt-2019.pdf) (EDBT 2019, Best Paper)
 - [Paper: Scotty: Efficient Window Aggregation for out-of-order Stream Processing](http://www.user.tu-berlin.de/powibol/assets/publications/traub-scotty-icde-2018.pdf) (ICDE 2018)
 - [Presentations Slides FlinkForward 2018](https://www.slideshare.net/powibol/flink-forward-2018-efficient-window-aggregation-with-stream-slicing)
 - [API Documentation](docs)
 - [Paper: Scotty: General and Efficient Open-source Window Aggregation for Stream Processing Systems](https://dl.acm.org/doi/pdf/10.1145/3433675) (ACM TODS 2021)
 - [Paper: Benson et al.: Disco: Efficient Distributed Window Aggregation](https://openproceedings.org/2020/conf/edbt/paper_300.pdf) (EDBT 2020)
 - [Repository: Disco: Stream Processing Engine for Distributed Window Aggregation](https://github.com/hpides/disco)
 - [Repository: Scotty Fork including Disco](https://github.com/lawben/scotty-window-processor)

### Flink Integration Example:

```java
// Instantiate Scotty window operator
KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> windowOperator =
new KeyedScottyWindowOperator<>(new SumWindowFunction());

// Add multiple windows to the same operator
windowOperator.addWindow(new TumblingWindow(WindowMeasure.Time, 1000));
windowOperator.addWindow(new SlidingWindow(WindowMeasure.Time, 1000, 5000));
windowOperator.addWindow(new SessionWindow(WindowMeasure.Time, 1000));

// Add operator to Flink job
stream.keyBy(0)
      .process(windowOperator)
```

More examples for Flink as well as demos for the other systems are provided in the [demo folder](demo).

### Benchmark:  


Throughput in comparison to the Flink standard window operator (Window Buckets) for Sliding Event-Time Windows:  
We fix the window size to 60 seconds and modify the slide size.
If the slide size gets smaller, Flink has to maintain a higher number of overlapping (concurrent) windows.

![](charts/SlidingWindow.png?raw=true)

Throughput in comparison to Flink for concurrent Tumbling Windows:

![](charts/ConcurrentTumblingWindows.png?raw=true)

### Out-of-order Processing in Scotty:

A watermark with timestamp t indicates that no tuple with a timestamp lower than t will arrive.
When a watermark arrives, Scotty outputs all window results of windows that ended before t. 

However, some out-of-order tuples may arrive with a timestamp t' lower than the watermark t (t' <= t).
For that case, the maxLateness indicates how long Scotty stores slices and their corresponding window aggregates.
Scotty processes an out-of-order tuple as long as its in the allowed lateness, 
i.e. it has an timestamp t' that is bigger than watermark minus the maxLateness (t' > t - maxLateness). 
Then, Scotty outputs updated aggregates for the windows.
The maxLateness can be adjusted with the method setMaxLateness of the slicingWindowOperator.

If an out-of-order tuple arrives outside the allowed lateness 
(with a timestamp t' < t - maxLateness - maxFixedWindowSize), it is discarded.
MaxFixedWindowSize is the maximum size of window types which have fixed sizes (e.g. tumbling window or 
sliding window).
For context-aware window types, the maxFixedWindowSize is 0.

If no watermark has arrived yet, an out-of-order tuple is outside the allowed lateness, 
when its timestamp is lower than the start timestamp of the first slice (t' < tsOfFirstSlice),
or in case of context aware windows, the timestamp of the first tuple minus the maxlateness 
(t' < tsOfFirstTuple - maxLateness).

### Roadmap:
We plan to extend our framework with the following features:

- Support for User-Defined windows
- User-defined window measures
- Support for Refinements
- Support of Flink Checkpoints and State Backends

### Setup:
The maven package is currently not publically available.
Therefore we have to build it from source:

`
git clone git@github.com:TU-Berlin-DIMA/scotty-window-processor.git
`

`
mvn clean install
`

Then you can use the library in your maven project.

```xml
<dependency> 
 <groupId>stream.scotty</groupId>
 <artifactId>flink-connector</artifactId>
 <version>0.4</version>
</dependency>
```

### Efficient Window Aggregation with General Stream Slicing at EDBT 2019
General Stream Slicing received the Best Paper Award at the [22nd International Conference on Extending Database Technology](http://edbticdt2019.inesc-id.pt/?awards_bp_edbt) in March 2019.  

**Abstract:**  
Window aggregation is a core operation in data stream processing. Existing aggregation techniques focus on reducing latency, eliminating redundant computations, and minimizing memory usage. However, each technique operates under different assumptions with respect to workload characteristics such as properties of aggregation functions (e.g., invertible, associative), window types (e.g., sliding, sessions), windowing measures (e.g., time- or countbased), and stream (dis)order. Violating the assumptions of a technique can deem it unusable or drastically reduce its performance.
In this paper, we present the first general stream slicing technique for window aggregation. General stream slicing automatically adapts to workload characteristics to improve performance without sacrificing its general applicability. As a prerequisite, we identify workload characteristics which affect the performance and applicability of aggregation techniques. Our experiments show that general stream slicing outperforms alternative concepts by up to one order of magnitude.

- Paper: [Efficient Window Aggregation with General Stream Slicing](http://www.user.tu-berlin.de/powibol/assets/publications/traub-efficient-window-aggregation-with-general-stream-slicing-edbt-2019.pdf)

- BibTeX citation:
```
@inproceedings{traub2019efficient,
  title={Efficient Window Aggregation with General Stream Slicing},
  author={Traub, Jonas and Grulich, Philipp M. and Cu{\'e}llar, Alejandro Rodr{\'\i}guez and Bre{\ss}, Sebastian and Katsifodimos, Asterios and Rabl, Tilmann and Markl, Volker},
  booktitle={22th International Conference on Extending Database Technology (EDBT)},
  year={2019}
}
```

Acknowledgements: This work was funded by the EU project E2Data (780245), Delft Data Science, and the German Ministry for Education and
Research as BBDC I (01IS14013A) and BBDC II (01IS18025A)

### Scotty: Efficient Window Aggregation for out-of-order Stream Processing at ICDE 2018
Scotty was first published at the [34th IEEE International Conference on Data Engineering](https://icde2018.org/) in April 2018.  

**Abstract:**  
Computing aggregates over windows is at the core
of virtually every stream processing job. Typical stream processing
applications involve overlapping windows and, therefore,
cause redundant computations. Several techniques prevent this
redundancy by sharing partial aggregates among windows. However,
these techniques do not support out-of-order processing and
session windows. Out-of-order processing is a key requirement
to deal with delayed tuples in case of source failures such as
temporary sensor outages. Session windows are widely used to
separate different periods of user activity from each other.
In this paper, we present Scotty, a high throughput operator
for window discretization and aggregation. Scotty splits streams
into non-overlapping slices and computes partial aggregates per
slice. These partial aggregates are shared among all concurrent
queries with arbitrary combinations of tumbling, sliding, and
session windows. Scotty introduces the first slicing technique
which (1) enables stream slicing for session windows in addition
to tumbling and sliding windows and (2) processes out-of-order
tuples efficiently. Our technique is generally applicable to a
broad group of dataflow systems which use a unified batch and
stream processing model. Our experiments show that we achieve
a throughput an order of magnitude higher than alternative stateof-the-art
solutions.

- Paper: [Scotty: Efficient Window Aggregation for out-of-order Stream Processing](http://www.user.tu-berlin.de/powibol/assets/publications/traub-scotty-icde-2018.pdf)

- BibTeX citation:
```
@inproceedings{traub2018scotty,
  title={Scotty: Efficient Window Aggregation for out-of-order Stream Processing},
  author={Traub, Jonas and Grulich, Philipp M. and Cuellar, Alejandro Rodríguez and Breß, Sebastian and Katsifodimos, Asterios and Rabl, Tilmann and Markl, Volker},
  booktitle={34th IEEE International Conference on Data Engineering (ICDE)},
  year={2018}
}
```

Acknowledgements: This work was supported by the EU projects Proteus (687691) and Streamline (688191), DFG Stratosphere (606902), and the German Ministry for Education and Research as BBDC (01IS14013A) and Software Campus (01IS12056).

### Scotty: General and Efficient Open-source Window Aggregation for Stream Processing Systems at ACM TODS 2021

**Abstract:**  
Window aggregation is a core operation in data stream processing. Existing aggregation techniques focus on
reducing latency, eliminating redundant computations, or minimizing memory usage. However, each technique 
operates under different assumptions with respect to workload characteristics, such as properties of
aggregation functions (e.g., invertible, associative), window types (e.g., sliding, sessions), windowing measures 
(e.g., time- or count-based), and stream (dis)order. In this article, we present Scotty, an efficient and
general open-source operator for sliding-window aggregation in stream processing systems, such as Apache
Flink, Apache Beam, Apache Samza, Apache Kafka, Apache Spark, and Apache Storm. One can easily extend
Scotty with user-defined aggregation functions and window types. Scotty implements the concept of general
stream slicing and derives workload characteristics from aggregation queries to improve performance without 
sacrificing its general applicability. We provide an in-depth view on the algorithms of the general stream
slicing approach. Our experiments show that Scotty outperforms alternative solutions

- Paper: [Scotty: General and Efficient Open-source Window Aggregation for Stream Processing Systems](https://dl.acm.org/doi/pdf/10.1145/3433675)

- BibTeX Citation:
```
@article{traub2021scotty,
  title={Scotty: General and Efficient Open-source Window Aggregation for Stream Processing Systems},
  author={Traub, Jonas and Grulich, Philipp Marian and Cu{\'e}llar, Alejandro Rodr{\'\i}guez and Bre{\ss}, Sebastian and Katsifodimos, Asterios and Rabl, Tilmann and Markl, Volker},
  journal={ACM Transactions on Database Systems (TODS)},
  volume={46},
  year={2021},
  publisher={ACM New York, NY, USA}
}
```

Acknowledgements: This work was supported by the German Ministry for Education and Research as BIFOLD (01IS18025A
                  and 01IS18037A), SFB 1404 FONDA, and the EU Horizon 2020 Opertus Mundi project (870228).

### Benson et al.: Disco: Efficient Distributed Window Aggregation

**Abstract:**  
Many business applications benefit from fast analysis of online
data streams. Modern stream processing engines (SPEs) provide
complex window types and user-defined aggregation functions to
analyze streams. While SPEs run in central data centers, wireless
sensors networks (WSNs) perform distributed aggregations close
to the data sources, which is beneficial especially in modern IoT
setups. However, WSNs support only basic aggregations and windows. 
To bridge the gap between complex central aggregations
and simple distributed analysis, we propose Disco, a distributed
complex window aggregation approach. Disco processes complex
window types on multiple independent nodes while efficiently
aggregating incoming data streams. Our evaluation shows that
Disco’s throughput scales linearly with the number of nodes
and that Disco already outperforms a centralized solution in a
two-node setup. Furthermore, Disco reduces the network cost
significantly compared to the centralized approach. 
Disco’s treelike topology handles thousands of nodes per level and 
scales to support future data-intensive streaming applications.

- Paper: [Disco: Efficient Distributed Window Aggregation](https://openproceedings.org/2020/conf/edbt/paper_300.pdf)

- BibTeX Citation:
```
@inproceedings{benson2020disco,
  title={Disco: Efficient Distributed Window Aggregation.},
  author={Benson, Lawrence and Grulich, Philipp M and Zeuch, Steffen and Markl, Volker and Rabl, Tilmann},
  booktitle={EDBT},
  volume={20},
  year={2020}
}
```