# Author: Gabriele Mencagli
# Date: 04/01/2023

FF_ROOT         = $(HOME)/fastflow
WF_INCLUDES     = $(HOME)/WindFlow-3.4.0/wf
INCLUDE_DIR     = ../includes
OUT_DIR         = ../bin

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR) -I../json/include
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG
OPTFLAGS        = -g -O3 -finline-functions
LDFLAGS         = -pthread -lmaxminddb

all: lp

lp.o: lp.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

lp: lp.o
	$(CXX) lp.o -o $(OUT_DIR)/lp $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/lp
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
