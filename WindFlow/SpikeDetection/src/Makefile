# Author: Gabriele Mencagli
# Date: 13/01/2020

FF_ROOT			= $(HOME)/fastflow
WF_INCLUDES		= $(HOME)/WindFlow/wf
INCLUDE_DIR		= ../includes
OUT_DIR			= ../bin

CXX 			= g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread

all: sd

metric.o: ../includes/util/metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

metric_group.o: ../includes/util/metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sampler.o: ../includes/util/sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sd.o: sd.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sd: metric.o metric_group.o sampler.o sd.o folder
	$(CXX) metric.o metric_group.o sampler.o sd.o -o ../bin/sd $(LDFLAGS)

folder:
	mkdir -p ../bin

clean:
	rm -f $(OUT_DIR)/*
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean folder
