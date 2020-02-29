# StreamBenchmarks
Suite of Benchmarks Applications for Stream Processing Systems

This repository includes a set of seven streaming applications taken from the literature and from existing repositories and cleaned up properly. All the applications can be run in an homogeneous manner and executions collect statistics of throughput and latency in different ways. The applications are:
* <strong>FraudDetection</strong> (FD) -> applies a Markov model to calculate the probability of a credit card transaction being a fraud
* <strong>SpikeDetection</strong> (SD) -> finds out the spikes in a stream of sensor readings using a moving-average operator of 1,000 events and a filter based on a fixed threshold
* <strong>TrafficMonitoring</strong> (TM) -> processes a stream of events emitted from taxis in the city of Beijing. An operator is responsible for identifying the road that vehicle is riding and another operator updates the average speed of vehicles for each road
* <strong>WordCount</strong> (WC) -> counts the number of instances of each word present in a text file
* <strong>Yahoo! Streaming Benchmark</strong> (YSB) -> emulates an advertisement application. The goal is to compute 10-seconds windowed counts of advertisement campaigns that have the same type
* <strong>LinearRoad</strong> (LR) -> emulates a tolling system for the vehicle expressways. The system uses a variable tolling technique accounting for traffic congestion and accident proximity to calculate toll charges
* <strong>VoipStream</strong> (VS) -> it has been used in the evaluation of Blockmon. It detects telemarketing users by analyzing call detail records using a set of Bloom filters

The seven applications are available in four different Stream Processing Systems (SPSs):
* <strong>Apache Storm</strong>
* <strong>Apache Flink</strong>
* <strong>BriskStream</strong> ([link](https://github.com/Xtra-Computing/briskstream))
* <strong>WindFlow</strong> ([link](https://github.com/ParaGroup/WindFlow)

Dataset files are quite large. For same applications, the scripts to generate them have been provided in this repository. For the other applications, send an email to the contributors of this repository.

# Contributors
The main developer and maintainer of this repository is [Gabriele Mencagli](mailto:mencagli@di.unipi.it).
