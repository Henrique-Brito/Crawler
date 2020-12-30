CC := g++
SRCDIR := src

SRCEXT := cpp
SOURCES := $(shell find $(SRCDIR) -type f -name *.$(SRCEXT))

CFLAGS := -g -O3 -Wall --std=c++17 -I/home/henrique/chilkat/include -L/home/henrique/chilkat/lib -lchilkat-9.5.0 -lpthread 

main: $(OBJECTS)
	$(CC) $(SOURCES) $(CFLAGS) $^ -o tp2

all: main

clean:
	$(RM) tp2

.PHONY: clean
