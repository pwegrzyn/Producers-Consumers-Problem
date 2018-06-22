CC := gcc
CFLAGS := -Wall -std=c99 -g
LDLIBS := -lpthread -lyaml

all: clean main

main: main.o
	$(CC) $(CFLAGS) -o main main.o $(LDLIBS)

main.o: main.c
	$(CC) $(CFLAGS) -c main.c

clean: 
	rm -f *.o main