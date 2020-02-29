# Compile and run the applications on BriskStream

## Compile
mvn validate
mvn clean install -DskipTests
Use Java8 for compiling and running the applications in BriskSteam.

## Run FraudDetection
Example: java -cp BriskBenchmarks/target/BriskBenchmarks-1.2.0-jar-with-dependencies.jar applications.BriskRunner -a FraudDetection -native -bt 1 --rate 0 --parallelism 1,1,1 --sampling 100

The first three parameters are mandatory for any application. The option -a states the application to be run, -native enables the native execution, and -bt is the size of the jambo tuple in terms of elementary tuples (in the example 1 means that jambo tuples are actually not used in the example).

We start the program with parallelism 1 for each operator (Source, Predictor and Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).

## Run SpikeDetection
Example: java -cp BriskBenchmarks/target/BriskBenchmarks-1.2.0-jar-with-dependencies.jar applications.BriskRunner -a SpikeDetection -native -bt 1 --rate 0 --parallelism 1,1,1,1 --sampling 100

We start the program with parallelism 1 for each operator (Source, Moving-Average, Spike-Detector and Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).

## Run TrafficMonitoring
Example: java -cp BriskBenchmarks/target/BriskBenchmarks-1.2.0-jar-with-dependencies.jar applications.BriskRunner -a TrafficMonitoring -native -bt 1 --rate 0 --parallelism 1,1,1,1 --sampling 100

We start the program with parallelism 1 for each operator (Source, Map-Matcher, Speed-Calculator and Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter). This application strongly requires Java8 to be correctly executed (otherwise some exceptions are generally raised in the GeoTools code).

## Run WordCount
Example: java -cp BriskBenchmarks/target/BriskBenchmarks-1.2.0-jar-with-dependencies.jar applications.BriskRunner -a WordCount -native -bt 1 --rate 0 --parallelism 1,1,1,1 --sampling 100

We start the program with parallelism 1 for each operator (Source, Splitter, Counter and Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).

## Run Yahoo! Streaming Benchmark
Example: java -cp BriskBenchmarks/target/BriskBenchmarks-1.2.0-jar-with-dependencies.jar applications.BriskRunner -a YSB -native -bt 1 -r 6 --rate 0 --parallelism 1,1,1,1,1 --sampling 0

We start the program with parallelism 1 for each operator (Source, Filter, Joiner, Win-Aggregate and Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).
