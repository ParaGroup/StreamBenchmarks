# Compile and run SpikeDetection

## Compile
mvn clean install

## Run
Example: java -cp target/SpikeDetection-1.0.jar SpikeDetection.SpikeDetection --rate 0 --sampling 100 --parallelism 1 1 1 1

In the example above, we start the program with parallelism 1 for each operator (Source, Moving-Average, Spike-Calculator, Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).
