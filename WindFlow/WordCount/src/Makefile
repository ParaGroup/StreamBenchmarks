# Author: Gabriele Mencagli
# Date: 13/01/2020

FF_ROOT         = $(HOME)/fastflow
WF_INCLUDES		= $(HOME)/WindFlow-4.0.0/wf
INCLUDE_DIR     = ../includes
OUT_DIR         = ../bin

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG
OPTFLAGS        = -g -O3 -finline-functions
LDFLAGS         = -pthread

all: wc

metric.o: ../includes/util/metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

metric_group.o: ../includes/util/metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sampler.o: ../includes/util/sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

wc.o: wc.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

wc: metric.o metric_group.o sampler.o wc.o
	$(CXX) metric.o metric_group.o sampler.o wc.o -o $(OUT_DIR)/wc $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/wc
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
