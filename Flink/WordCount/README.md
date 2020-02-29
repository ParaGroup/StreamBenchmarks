# Compile and run WordCount

## Compile
mvn clean install

## Run
Example: java -cp target/WordCount-1.0.jar WordCount.WordCount --rate 0 --sampling 100 --parallelism 1 1 1 1 [--chaining]

In the example above, we start the program with parallelism 1 for each operator (Source, Splitter, Counter, Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).
