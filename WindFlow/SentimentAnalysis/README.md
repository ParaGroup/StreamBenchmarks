# Compile and run SentimentAnalysis

## Dependencies
apt install libmaxminddb-dev

## Run

Example: ./bin/sa --rate 0 --sampling 100 --batch 9--parallelism <1,1,1> [--chaining]

In the example above, we start the program with parallelism 1 for each operator (Source, Classifier and Sink separated by commas in the --parallelism attribute). Latency values are gathered every 100 received tuples in the Sink (--sampling parameter), while the generation is performed at full speed (value 0 for the --rate parameter). The --batch parameter can be used to apply an output batching from each operator: 0 means no batching, values greater than 0 switch WindFlow in batch mode.
