# Compile and run FraudDetection

## Compile
make all

## Run
Example: ./bin/fd --rate 0 --keys 0 --sampling 100 --batch 0 --parallelism 1,1,1 [--chaining]

In the example above, we start the program with parallelism 1 for each operator (Source, Predictor and Sink separated by commas). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter). The --batch parameter can be used to apply an output batching from each operator: 0 means no batching, values greater than 0 switch WindFlow in batch mode. The attribute --keys indicates the number of keys to use: zero means to use the default number of keys of the dataset, otherwise a specific number of keys (uniformely distributed) can be used.
