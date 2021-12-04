[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![Hits](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2FParaGroup%2FStreamBenchmarks&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%232F84E1&title=hits&edge_flat=false)](https://hits.seeyoufarm.com)

# StreamBenchmarks

This repository includes a set of seven streaming applications taken from the literature, and from existing repositories (e.g., [here](https://github.com/GMAP/DSPBench)), which have been cleaned up properly. All the applications can be run in a homogeneous manner and their execution collects statistics of throughput and latency in different ways. The applications are:
* <strong>FraudDetection</strong> (FD) -> applies a Markov model to calculate the probability of a credit card transaction being a fraud
* <strong>SpikeDetection</strong> (SD) -> finds out the spikes in a stream of sensor readings using a moving-average operator of 1,000 events and a filter based on a fixed threshold
* <strong>TrafficMonitoring</strong> (TM) -> processes a stream of events emitted from taxis in the city of Beijing. An operator is responsible for identifying the road that vehicle is riding and another operator updates the average speed of vehicles for each road
* <strong>WordCount</strong> (WC) -> counts the number of instances of each word present in a text file
* <strong>Yahoo! Streaming Benchmark</strong> (YSB) -> emulates an advertisement application. The goal is to compute 10-seconds windowed counts of advertisement campaigns that have the same type
* <strong>LinearRoad</strong> (LR) -> emulates a tolling system for the vehicle expressways. The system uses a variable tolling technique accounting for traffic congestion and accident proximity to calculate toll charges
* <strong>VoipStream</strong> (VS) -> it has been used in the evaluation of Blockmon. It detects telemarketing users by analyzing call detail records using a set of Bloom filters

The seven applications are available in three different Stream Processing Systems (SPSs):
* <strong>Apache Storm</strong>
* <strong>Apache Flink</strong>
* <strong>WindFlow</strong> ([link](https://github.com/ParaGroup/WindFlow))

For WindFlow, benchmarks are available also for the new release 3.x having a slightly different API than WindFlow version 2.x.

The same applications (except YSB) have also been provided in <strong>BriskStream</strong>, a research SPS for multicores. They can be found [here](https://github.com/Xtra-Computing/briskstream). If you need to test YSB in BriskStream, send an email to me and I will share the source code with you.

Dataset files are quite large. For some applications, the scripts to generate them have been provided in this repository. For the other applications, send me an email.

# References
This repository uses the applications that we have recently added to a larger benchmark suite of streaming applications called <tt>DSPBench</tt> available on GitHub at the following [link](https://github.com/GMAP/DSPBench). If our applications revealed useful for your research, we kindly ask you to give credit to our effort by citing the following paper:
```
@article{DSPBench,
 author={Bordin, Maycon Viana and Griebler, Dalvan and Mencagli, Gabriele and Geyer, Cláudio F. R. and Fernandes, Luiz Gustavo L.},
 journal={IEEE Access}, 
 title={DSPBench: A Suite of Benchmark Applications for Distributed Data Stream Processing Systems}, 
 year={2020},
 volume={8},
 number={},
 pages={222900-222917},
 doi={10.1109/ACCESS.2020.3043948}
}
```

# Contributors
The main developer and maintainer of this repository is [Gabriele Mencagli](mailto:mencagli@di.unipi.it). Other authors of the source code are Alessandra Fais and Andrea Cardaci.
