# Compile and run ReinforcementLearner
Please install the stduuid library in the stduuid folder.

## Dependencies
apt install libmaxminddb-dev

## Run

Example: ./bin/rl --rate 0 --sampling 100 --batch 0 --parallelism <1,1,1,1> [--chaining]

In the example above, we start the program with parallelism 1 for each operator (Event-Source, Reward-Source, Learner, and Sink separated by commas in the --parallelism attribute). Latency values are gathered every 100 received tuples in the Sink (--sampling parameter), while the generation is performed at full speed (value 0 for the --rate parameter). The --batch parameter can be used to apply an output batching from each operator: 0 means no batching, values greater than 0 switch WindFlow in batch mode.

The Learner operator comese with alternative implementations that cannot be changed using command line arguments at the moment. You can easily edit the rl.cpp source file to choose the right version before compiling.
