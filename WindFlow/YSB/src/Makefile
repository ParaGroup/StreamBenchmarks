# Author: Gabriele Mencagli
# Date: 13/01/2020

FF_ROOT			= $(HOME)/fastflow
WF_INCLUDES		= $(HOME)/WindFlow/wf
INCLUDE_DIR		= ../includes
OUT_DIR			= ../bin

CXX 			= g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG -DN_CAMPAIGNS=100
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread

all: ysb

metric.o: ../includes/util/metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

metric_group.o: ../includes/util/metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sampler.o: ../includes/util/sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

ysb.o: ysb.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

ysb: metric.o metric_group.o sampler.o ysb.o folder
	$(CXX) metric.o metric_group.o sampler.o ysb.o -o ../bin/ysb $(LDFLAGS)

folder:
        mkdir -p ../bin

clean:
	rm -f $(OUT_DIR)/*
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean folder
