# Compile and run TaxiQueries

## Compile
mvn clean install

## Run
Example: $ sudo ./bin/start-cluster.sh
         $ ./bin/flink run /home/mencagli/StreamBenchmarks/Flink/TaxiQueries/target/TaxiQueries-0.1.jar -q <query_id> -p <parallelism> [-r]
         $ sudo ./bin/stop-cluster.sh

This implementation supports three queries with the TaxiRide datasets. It accepts as command-line arguments the query identifier, the parallelism (used by all operators) and the option to use RocksDB as the state back-end (option -r). The supported queries are:
    1: for each city area, compute the median number of passengers of all rides ending in that area in a window W1 sliding every S1;
    2: for each city area, counts the number of unique locations visited by all rides in a window W2 sliding every S2;
    3: for each city area, compute the inter-quartile range (IQR) of distances within a window W3 sliding every S3.
The parameters W[1..3] and S[1..3] are cabled in the file TaxiQueriesJob.java as constants.

sudo apt-get install zlib1g-dev