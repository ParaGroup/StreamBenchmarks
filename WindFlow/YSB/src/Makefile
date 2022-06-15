# Author: Gabriele Mencagli
# Date: 13/01/2020

FF_ROOT			= $(HOME)/fastflow
WF_INCLUDES		= $(HOME)/WindFlow-3.2.0/wf
INCLUDE_DIR		= ../includes
OUT_DIR			= ../bin

CXX 			= g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG -DN_CAMPAIGNS=100
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread

all: yb

metric.o: ../includes/util/metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

metric_group.o: ../includes/util/metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sampler.o: ../includes/util/sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

yb.o: yb.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

yb: metric.o metric_group.o sampler.o yb.o
	$(CXX) metric.o metric_group.o sampler.o yb.o -o ../bin/yb $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/yb
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
