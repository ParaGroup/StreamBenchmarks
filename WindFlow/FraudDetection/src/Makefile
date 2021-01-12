# Author: Gabriele Mencagli
# Date: 11/01/2020

FF_ROOT			= $(HOME)/fastflow
WF_INCLUDES		= $(HOME)/WindFlow/wf
INCLUDE_DIR		= ../includes
OUT_DIR			= ../bin

CXX 			= g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 #-DNDEBUG (strange better to remove this macro for FD)
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread

all: fd

metric.o: ../includes/util/metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

metric_group.o: ../includes/util/metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sampler.o: ../includes/util/sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

fd.o: fd.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

fd: metric.o metric_group.o sampler.o fd.o folder
	$(CXX) metric.o metric_group.o sampler.o fd.o -o ../bin/fd $(LDFLAGS)

folder:
	mkdir -p ../bin

clean:
	rm -f $(OUT_DIR)/*
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean folder
