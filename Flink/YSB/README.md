# Compile and run YSB

## Compile
mvn clean install

## Run
Example: java -cp target/YSB-1.0.jar YSB.YSB --rate 0 --sampling 0 --parallelism 1 1 1 1 1 [--chaining]

In the example above, we start the program with parallelism 1 for each operator (Source, Filter, Joiner, Aggregate, Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).
