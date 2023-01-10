# Compile and run MachineOutlier

## Dependencies
apt install libmaxminddb-dev

## Run

Example: ./bin/mo --rate 0 --sampling 100 --parallelism <1,1,1,1,1> [--chaining]

In the example above, we start the program with parallelism 1 for each operator (Source, Observation-Scorer, Anomaly-Scorer, Alert-Triggerer and Sink separated by commas in the --parallelism attribute). Latency values are gathered every 100 received tuples in the Sink (--sampling parameter), while the generation is performed at full speed (value 0 for the --rate parameter).

Most of the operators of this application come with alternative implementations that cannot be changed using command line arguments at the moment. You can easily edit the mo.cpp source file to choose the right version before compiling.
