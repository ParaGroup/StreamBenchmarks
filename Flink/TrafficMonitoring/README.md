# Compile and run TrafficMonitoring

## Compile
mvn clean install

## Run
Example: java -cp target/TrafficMonitoring-1.0.jar TrafficMonitoring.TrafficMonitoring --rate 0 --sampling 100 --parallelism 1 1 1 1 [--chaining]

In the example above, we start the program with parallelism 1 for each operator (Source, Map-Matcher, Speed-Calculator, Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter).

Important: we have experienced some exceptions raised if TrafficMonitoring is executed with Java>8. They are generated in the GeoTools source code which is not our responsibility. So, please use Java8 for running TrafficMonitoring.
