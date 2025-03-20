# Author: Gabriele Mencagli
# Date: 26/02/2025

FF_ROOT         = $(HOME)/fastflow
WF_INCLUDES     = $(HOME)/WindFlow-4.2.2/wf
INCLUDE_DIR     = ../includes
OUT_DIR         = ../bin

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=2048 -DNDEBUG
OPTFLAGS        = -g -O3 -finline-functions
LDFLAGS         = -pthread -lz -lrocksdb

all: taxi_queries

taxi_queries.o: taxi_queries.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

taxi_queries: taxi_queries.o
	$(CXX) taxi_queries.o -o $(OUT_DIR)/taxi_queries $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/taxi_queries
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
