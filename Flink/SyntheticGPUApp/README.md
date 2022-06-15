# Compile and run SyntheticGPUApp

## Compile
mvn clean install

## Run
Example: $ sudo ./bin/start-cluster.sh
         $ ./bin/flink run /<path>/StreamBenchmarks/Flink/SyntheticGPUApp/target/SyntheticGPUApp-0.1.jar <parallelism> <bath_size> <duration_sec>

This application needs to start a local Flink cluster and to deploy the compiled jar using the Flink runner client. The application expects three command-line arguments: the first is the parallelism of the operators, the second is the batch size, and the last one is the duration of the test in seconds. The application is a stateless pipeline with three operators: a Source, a Map and a Sink.
