# Author: Gabriele Mencagli
# Date: 11/01/2020

all: bin
	$(MAKE) -C src

clean:
	$(RM) -r bin && $(MAKE) clean -C src

bin:
	@mkdir -p bin

.DEFAULT_GOAL := all
.PHONY: all clean
