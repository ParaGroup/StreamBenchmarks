# Author: Gabriele Mencagli
# Date: 03/01/2023

FF_ROOT         = $(HOME)/fastflow
WF_INCLUDES		= $(HOME)/WindFlow-4.0.0/wf
INCLUDE_DIR     = ../includes
OUT_DIR         = ../bin
SRCS            = $(wildcard *.cpp)
OBJS            = $(patsubst %.cpp,%.o,$(SRCS))

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG
OPTFLAGS        = -g -O3 -finline-functions
LDFLAGS         = -pthread -lcrypto

all: lr

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

lr: $(OBJS)
	$(CXX) $(OBJS) -o $(OUT_DIR)/lr $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/lr
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
